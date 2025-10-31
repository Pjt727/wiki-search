use dashmap::DashMap;
use lasso::*;
use lasso::{Spur, ThreadedRodeo};
use ordered_float::OrderedFloat;
use rand::rng;
use rand::seq::{IndexedRandom, SliceRandom};
use rayon::prelude::*;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
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
            let entries: Vec<ZimEntry> = entry_iter
                .by_ref()
                .map(|e| e.unwrap())
                .take(5_000)
                .collect();
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
    fn get_close_titles(
        &self,
        first_link: Spur,
        count: usize,
        min_distance: f32,
        max_distance: f32,
    ) -> Vec<(Page, f32)> {
        let mut link_to_distance: HashMap<Spur, OrderedFloat<f32>> = HashMap::new();
        let mut next_pages = BinaryHeap::new();
        // add the first page
        next_pages.push(PrioritizedPage {
            priority: Reverse(OrderedFloat(-1_f32)),
            link: first_link,
        });

        while let Some(p) = next_pages.pop() {
            let link_page = self.link_to_page.get(&p.link).unwrap();

            for (link, info) in link_page.value().links_to_weight.clone() {
                let total_distance = p.priority.0 .0 + info.weight;
                if total_distance + 1_f32 > max_distance {
                    continue;
                }
                let prio_page = PrioritizedPage {
                    priority: Reverse(OrderedFloat(total_distance + 1_f32)),
                    link,
                };
                if total_distance > min_distance && total_distance < max_distance {
                    match link_to_distance.entry(link) {
                        Entry::Occupied(mut occupied_entry) => {
                            let current_score = occupied_entry.get_mut();
                            if OrderedFloat(total_distance) < *current_score {
                                println!("Found a new fastest route to {}", self.resolve(link));
                                *current_score = OrderedFloat(total_distance);
                                next_pages.push(prio_page);
                            }
                        }
                        Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(OrderedFloat(total_distance));
                            next_pages.push(prio_page);
                        }
                    }
                }
            }
        }

        let canidates: Vec<_> = link_to_distance.keys().collect();

        let mut rng = rng();
        canidates
            .choose_multiple(&mut rng, count)
            .map(|link| {
                let link_page = self.link_to_page.get(link).unwrap();
                let distance = link_to_distance.get(link).unwrap();
                (link_page.value().clone(), distance.0)
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct PrioritizedPage {
    pub priority: Reverse<OrderedFloat<f32>>,
    pub link: Spur,
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

fn main() {
    let file_path = "wikipedia_en_medicine_nopic_2025-09.zim";
    // let mut wiki_graph = WikiGraph::new(file_path);
    let wiki_graph = WikiGraph::load_bin(file_path).unwrap();
    println!("Loaded arctiles: {}", wiki_graph.link_to_page.len());
    let mut links = wiki_graph.link_to_page.iter();
    let link = links.nth(200).unwrap();
    println!("Entry: {}", wiki_graph.resolve(*link.key()));
    wiki_graph.get_close_titles(*link.key(), 5, 1.0, 3.0);
    // wiki_graph.get_all();
    // wiki_graph.save_bin().unwrap();
}
