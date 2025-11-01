use dashmap::DashMap;
use lasso::*;
use lasso::{Spur, ThreadedRodeo};
use ordered_float::OrderedFloat;
use rand::rng;
use rand::seq::IndexedRandom;
use rayon::prelude::*;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::task::Wake;
use std::time::Instant;
use zim_rs::archive::Archive;
use zim_rs::entry::Entry as ZimEntry;

const WIKI_GRAPH_PATH: &str = "wiki-graph";
const INTERNER_PATH: &str = "wiki-interner";

fn hash_to_dash<K, V>(hm: HashMap<K, V>) -> DashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    let dm = DashMap::new();
    for (k, v) in hm {
        dm.insert(k, v);
    }
    dm
}

fn dash_to_hash<K, V>(dm: &DashMap<K, V>) -> HashMap<K, V>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
{
    dm.iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LinkInfo {
    index: usize,
    weight: f32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Page {
    links_to_weight: HashMap<Spur, LinkInfo>,
}

#[derive(Debug, Clone)]
struct PathInfo {
    distance: f32,
    path: Vec<Spur>,
}

impl Page {
    fn from_entry(e: ZimEntry, interner: &ThreadedRodeo) -> Option<Self> {
        let i = e.get_item(true).ok()?;
        let blob = i.get_data().ok()?;
        let d = blob.data();
        let doc = match String::from_utf8(d.to_vec()) {
            Ok(s) => Html::parse_document(&s),
            Err(_) => return None,
        };
        let selector = Selector::parse("a[href]").ok()?;
        let all_links = doc
            .select(&selector)
            .filter_map(|e| e.attr("href"))
            .filter(|href| {
                !href.starts_with("http") && !href.starts_with("#") && !href.starts_with("../")
            })
            .fold(
                (HashSet::new(), Vec::new()),
                |(mut seen_paths, mut paths), p| {
                    let did_add = seen_paths.insert(p);
                    if did_add {
                        // Intern the string and store the Spur key
                        paths.push(interner.get_or_intern(p))
                    }
                    (seen_paths, paths)
                },
            )
            .1;

        let total_links = all_links.len();
        let links_to_weight =
            all_links
                .iter()
                .enumerate()
                .fold(HashMap::new(), |mut acc, (i, spur)| {
                    let weight = linear_distance(i + 1, total_links);
                    acc.entry(*spur).or_insert(LinkInfo { index: i, weight });
                    acc
                });
        Some(Page { links_to_weight })
    }
}

fn linear_distance(i: usize, total: usize) -> f32 {
    i as f32 / total as f32
}

pub struct WikiGraph {
    pub a: Archive,
    link_to_page: DashMap<Spur, Page>,
    interner: Arc<ThreadedRodeo>,
}

impl WikiGraph {
    pub fn new(file_path: &str) -> Self {
        let a = Archive::new(file_path).unwrap();
        WikiGraph {
            a,
            link_to_page: DashMap::new(),
            interner: Arc::new(ThreadedRodeo::new()),
        }
    }

    pub fn add_link(&mut self, link: &str) -> bool {
        let link_key = self.interner.get_or_intern(link);
        if self.link_to_page.contains_key(&link_key) {
            return false;
        }
        if let Ok(e) = self.a.get_entry_bypath_str(link)
            && let Some(page) = Page::from_entry(e, &self.interner)
        {
            self.link_to_page.insert(link_key, page);
            return true;
        }
        false
    }

    pub fn get_all(&mut self) {
        let start = Instant::now();
        let mut entry_iter = self.a.iter_efficient().unwrap().into_iter();
        let mut count = 0;
        loop {
            let entries: Vec<ZimEntry> =
                entry_iter.by_ref().map(|e| e.unwrap()).take(100).collect();
            if entries.is_empty() {
                break;
            }
            count += entries.len();
            println!("{}", count);

            let interner = Arc::clone(&self.interner);
            entries.into_iter().par_bridge().for_each(|e| {
                let path = e.get_path();
                if let Some(p) = Page::from_entry(e, &interner) {
                    let path_key = interner.get_or_intern(&path);
                    self.link_to_page.insert(path_key, p);
                }
            })
        }
        let duration = Instant::now().duration_since(start);
        dbg!(duration);
        dbg!(self.link_to_page.len());
        dbg!(self.interner.len());
    }

    pub fn save_bin(&self) -> std::io::Result<()> {
        // Convert interner to a Vec of strings for serialization
        let strings: Vec<String> = (0..self.interner.len())
            .map(|i| {
                let spur = Spur::try_from_usize(i).unwrap();
                self.interner.resolve(&spur).to_string()
            })
            .collect();

        // Save the graph
        let encoded = bincode::serde::encode_to_vec(
            dash_to_hash(&self.link_to_page),
            bincode::config::standard(),
        )
        .unwrap();
        std::fs::write(WIKI_GRAPH_PATH, encoded)?;

        // Save the interner as a string vector
        let interner_encoded =
            bincode::encode_to_vec(&strings, bincode::config::standard()).unwrap();
        std::fs::write(INTERNER_PATH, interner_encoded)?;

        Ok(())
    }

    pub fn load_bin(zim_path: &str) -> std::io::Result<Self> {
        let a = Archive::new(zim_path).unwrap();

        // Load and reconstruct the interner
        let interner_bytes = std::fs::read(INTERNER_PATH)?;
        let strings: Vec<String> =
            bincode::serde::decode_from_slice(&interner_bytes, bincode::config::standard())
                .unwrap()
                .0;

        let interner = ThreadedRodeo::new();
        for s in strings {
            interner.get_or_intern(s);
        }

        // Load the graph
        let bytes = std::fs::read(WIKI_GRAPH_PATH)?;
        let link_to_page: HashMap<Spur, Page> =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap()
                .0;

        Ok(WikiGraph {
            a,
            link_to_page: hash_to_dash(link_to_page),
            interner: Arc::new(interner),
        })
    }

    // Helper method to resolve interned strings
    pub fn resolve(&self, key: Spur) -> &str {
        self.interner.resolve(&key)
    }

    pub fn get_random_article(&self) -> Option<Spur> {
        let entry = self.a.get_randomentry().ok()?;
        let path = entry.get_path();
        Some(self.interner.get_or_intern(&path))
    }

    fn get_close_titles(
        &self,
        first_link: Spur,
        count: usize,
        min_distance: f32,
        max_distance: f32,
    ) -> Vec<PathInfo> {
        let mut link_to_path_info: HashMap<Spur, PathInfo> = HashMap::new();
        let mut visited: HashSet<Spur> = HashSet::new();
        let mut next_pages = BinaryHeap::new();
        // add the first page
        next_pages.push(PrioritizedPage {
            priority: Reverse(OrderedFloat(0.0)),
            link: first_link,
            path: vec![first_link],
        });

        while let Some(p) = next_pages.pop() {
            // Skip if we've already processed this node with a shorter distance
            if !visited.insert(p.link) {
                continue;
            }

            let link_page = self.link_to_page.get(&p.link);
            if link_page.is_none() {
                continue;
            }
            let link_page = link_page.unwrap();

            for (link, info) in link_page.value().links_to_weight.clone() {
                let total_distance = p.priority.0.0 + info.weight + 1_f32;
                if total_distance > max_distance {
                    continue;
                }

                // Skip if already visited with a shorter path
                if visited.contains(&link) {
                    continue;
                }

                let mut new_path = p.path.clone();
                new_path.push(link);

                if total_distance >= min_distance && total_distance <= max_distance {
                    link_to_path_info.entry(link).or_insert(PathInfo {
                        distance: total_distance,
                        path: new_path.clone(),
                    });
                }

                next_pages.push(PrioritizedPage {
                    priority: Reverse(OrderedFloat(total_distance)),
                    link,
                    path: new_path,
                });
            }
        }

        let candidates: Vec<_> = link_to_path_info.values().cloned().collect();
        println!("candidate count {}", candidates.len());

        let mut rng = rng();
        candidates
            .choose_multiple(&mut rng, count)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct PrioritizedPage {
    pub priority: Reverse<OrderedFloat<f32>>,
    pub link: Spur,
    pub path: Vec<Spur>,
}

impl PartialEq for PrioritizedPage {
    fn eq(&self, other: &Self) -> bool {
        self.priority.eq(&other.priority)
    }
}

impl Eq for PrioritizedPage {}

impl PartialOrd for PrioritizedPage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.priority.partial_cmp(&other.priority)
    }
}

impl Ord for PrioritizedPage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

fn closest_members() {
    let file_path = "wikipedia_en_medicine_nopic_2025-10.zim";
    // let mut wiki_graph = WikiGraph::new(file_path);
    let wiki_graph = WikiGraph::load_bin(file_path).unwrap();
    println!("Loaded articles: {}", wiki_graph.link_to_page.len());

    // Try to get a random starting article that exists in the graph
    let random_start = loop {
        if let Some(candidate) = wiki_graph.get_random_article() {
            match wiki_graph.link_to_page.contains_key(&candidate)
                && !wiki_graph
                    .link_to_page
                    .get(&candidate)
                    .unwrap()
                    .links_to_weight
                    .is_empty()
            {
                true => break candidate,
                false => continue,
            };
        };
    };

    println!(
        "\nRandom starting article: {}",
        wiki_graph.resolve(random_start)
    );

    // Find close articles within distance range
    let close_articles = wiki_graph.get_close_titles(random_start, 10, 2.0, 10.0);

    println!(
        "\nFound {} articles within distance 1.0-3.0:",
        close_articles.len()
    );

    // Display paths to each close article
    for path_info in close_articles {
        let target = path_info.path.last().unwrap();
        println!(
            "\n--- Path to: {} (distance: {:.2}) ---",
            wiki_graph.resolve(*target),
            path_info.distance
        );

        for (i, link) in path_info.path.iter().enumerate() {
            println!("  {}. {}", i + 1, wiki_graph.resolve(*link));
        }
    }
}

fn get_all() {
    let file_path = "wikipedia_en_medicine_nopic_2025-10.zim";
    let mut wiki_graph = WikiGraph::new(file_path);

    wiki_graph.get_all();
    wiki_graph.save_bin().unwrap();
}

fn main() {
    closest_members();
    // get_all();
}
