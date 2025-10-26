use std::collections::{HashMap, HashSet};

use bincode::{Decode, Encode};
use dashmap::DashMap;
use lasso::ThreadedRodeo;
use rayon::prelude::*;
use scraper::{Html, Selector};
use std::time::Instant;
use std::{sync::Arc, thread};
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
    fn from_entry(e: Entry) -> Option<Self> {
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
    link_to_page: DashMap<String, Page>,
}

impl WikiGraph {
    pub fn new(file_path: &str) -> Self {
        let a = Archive::new(file_path).unwrap();
        WikiGraph {
            a,
            link_to_page: DashMap::new(),
        }
    }
    pub fn add_link(&mut self, link: &str) -> bool {
        if self.link_to_page.contains_key(link) {
            return false;
        }
        if let Ok(e) = self.a.get_entry_bypath_str(link)
            && let Some(page) = Page::from_entry(e)
        {
            self.link_to_page.insert(link.to_string(), page);
            return true;
        }
        false
    }

    pub fn get_all(&mut self) {
        let start = Instant::now();
        let mut entry_iter = self.a.iter_efficient().unwrap().into_iter();
        let mut count = 0;
        loop {
            let entries: Vec<Entry> = entry_iter
                .by_ref()
                .map(|e| e.unwrap())
                .take(5_000)
                .collect();
            if entries.is_empty() {
                break;
            }
            count += entries.len();
            println!("{}", count);

            entries.into_iter().par_bridge().for_each(|e| {
                let path = e.get_path();
                if let Some(p) = Page::from_entry(e) {
                    self.link_to_page.insert(path, p);
                }
            })
        }
        let duration = Instant::now().duration_since(start);
        dbg!(duration);
        dbg!(self.link_to_page.len());
    }

    pub fn save_bin(&self) -> std::io::Result<()> {
        let encoded = bincode::encode_to_vec(
            dash_to_hash(&self.link_to_page),
            bincode::config::standard(),
        )
        .unwrap();
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
        Ok(WikiGraph {
            a,
            link_to_page: hash_to_dash(link_to_page),
        })
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
    wiki_graph.save_bin().unwrap();
}
