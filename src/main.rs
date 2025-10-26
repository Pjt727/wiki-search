use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use dashmap::DashMap;
use rayon::prelude::*;
use scraper::{Html, Selector};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use zim_rs::archive::Archive;
use zim_rs::entry::Entry;

const WIKI_GRAPH_PATH: &str = "wiki-graph";

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

#[derive(Debug, Clone, Decode, Encode)]
struct LinkInfo {
    index: usize,
    weight: f32,
}

#[derive(Debug, Clone, Decode, Encode)]
struct Page {
    all_links: Vec<String>,
    links_to_weight: HashMap<String, LinkInfo>,
}

impl Page {
    fn from_entry(e: Entry, a: &Archive) -> Option<Self> {
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
            // Assume it is ok
            // .filter_map(|e| a.get_entry_bypath_str(e.attr("href")?).ok())
            .filter_map(|e| e.attr("href"))
            .fold(
                (HashSet::new(), Vec::new()),
                |(mut seen_paths, mut paths), p| {
                    let did_add = seen_paths.insert(p);
                    if did_add {
                        paths.push(p.to_string())
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
                .fold(HashMap::new(), |mut acc, (i, p)| {
                    let weight = linear_distance(i + 1, total_links);
                    acc.entry(p.clone())
                        .or_insert(LinkInfo { index: i, weight });
                    acc
                });
        Some(Page {
            links_to_weight,
            all_links,
        })
    }
}

fn linear_distance(i: usize, total: usize) -> f32 {
    i as f32 / total as f32
}

pub struct WikiGraph {
    pub a: Archive,
    link_to_page: HashMap<String, Page>,
}

impl WikiGraph {
    pub fn new(file_path: &str) -> Self {
        let a = Archive::new(file_path).unwrap();
        WikiGraph {
            a,
            link_to_page: HashMap::new(),
        }
    }
    pub fn add_link(&mut self, link: &str) -> bool {
        if self.link_to_page.contains_key(link) {
            return false;
        }
        if let Ok(e) = self.a.get_entry_bypath_str(link)
            && let Some(page) = Page::from_entry(e, &self.a)
        {
            self.link_to_page.insert(link.to_string(), page);
            return true;
        }
        false
    }

    pub fn expand(&mut self) {
        dbg!(self.link_to_page.len());
        let mut links: HashSet<String> = self
            .link_to_page
            .iter()
            .flat_map(|p| p.1.all_links.to_vec())
            .collect();
        while !links.is_empty() {
            dbg!(links.len());

            links = links
                .iter()
                .filter_map(|link| {
                    if self.link_to_page.contains_key(link) {
                        return None;
                    }
                    if let Ok(e) = self.a.get_entry_bypath_str(link)
                        && let Some(page) = Page::from_entry(e, &self.a)
                    {
                        let links_for_this = page
                            .links_to_weight
                            .keys()
                            .filter(|s| !self.link_to_page.contains_key(*s))
                            .cloned()
                            .collect::<HashSet<String>>();
                        self.link_to_page.insert(link.to_string(), page);
                        return Some(links_for_this);
                    }
                    None
                })
                .flatten()
                .collect::<HashSet<String>>();
        }
    }

    pub fn get_all(&mut self) {
        let mut entry_err_count = 0;
        let mut page_err_count = 0;
        let start = Instant::now();
        for (i, e) in self.a.iter_efficient().unwrap().into_iter().enumerate() {
            if i % 1000 == 0 {
                println!("{}", i);
            }
            if i >= 10_000 {
                break;
            }
            if let Ok(entry) = e {
                let path = entry.get_path();
                if let Some(p) = Page::from_entry(entry, &self.a) {
                    self.link_to_page.insert(path, p);
                } else {
                    page_err_count += 1;
                }
            } else {
                entry_err_count += 1;
            }
        }
        let duration = Instant::now().duration_since(start);
        dbg!(duration);
        dbg!(page_err_count, entry_err_count);
        dbg!(self.link_to_page.len());
    }

    pub fn save_bin(&self) -> std::io::Result<()> {
        let encoded =
            bincode::encode_to_vec(&self.link_to_page, bincode::config::standard()).unwrap();
        std::fs::write(WIKI_GRAPH_PATH, encoded)?;
        Ok(())
    }

    pub fn load_bin(zim_path: &str) -> std::io::Result<Self> {
        let a = Archive::new(zim_path).unwrap();
        let bytes = std::fs::read(WIKI_GRAPH_PATH)?;
        let link_to_page: HashMap<String, Page> =
            bincode::decode_from_slice(&bytes, bincode::config::standard())
                .unwrap()
                .0;
        Ok(WikiGraph { a, link_to_page })
    }
}

fn main() {
    let file_path = "wikipedia_en_medicine_nopic_2025-09.zim";
    let mut wiki_graph = WikiGraph::new(file_path);
    wiki_graph.get_all();
    // dbg!(wiki_graph.a.get_articlecount());
    //
    // let e = wiki_graph.a.get_randomentry().unwrap();
    // dbg!(e.get_title());
    // wiki_graph.add_link(&e.get_path());
    // wiki_graph.expand();
    // wiki_graph.save_bin().unwrap();
}
