use std::collections::{HashMap, HashSet};

use rayon::prelude::*;
use scraper::{Html, Selector};
use zim_rs::archive::Archive;
use zim_rs::entry::Entry;
use zim_rs::search::{Query, Searcher};

#[derive(Debug, Clone)]
struct LinkInfo {
    index: usize,
    weight: f32,
}

#[derive(Debug)]
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
            .filter_map(|e| a.get_entry_bypath_str(e.attr("href")?).ok())
            .map(|e| e.get_path())
            .fold(
                (HashSet::new(), Vec::new()),
                |(mut seen_paths, mut paths), p| {
                    let did_add = seen_paths.insert(p.clone());
                    if did_add {
                        paths.push(p)
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
    pub fn expand(&mut self, steps: usize) {
        dbg!(self.link_to_page.len());
        if steps == 0 {
            return;
        }

        let links: Vec<String> = self
            .link_to_page
            .values()
            .flat_map(|p| p.all_links.iter().cloned())
            .collect();

        dbg!(links.len());

        // Parallel section
        let new_pages: Vec<(String, Page)> = links
            .par_iter()
            .filter_map(|link| {
                // Skip if already loaded
                if self.link_to_page.contains_key(link) {
                    return None;
                }
                if let Ok(e) = self.a.get_entry_bypath_str(link)
                    && let Some(page) = Page::from_entry(e, &self.a)
                {
                    Some((link.clone(), page))
                } else {
                    None
                }
            })
            .collect();

        // Sequential insertion (to avoid concurrent mutation)
        for (link, page) in new_pages {
            self.link_to_page.insert(link, page);
        }

        self.expand(steps - 1);
    }

    pub fn expand(&mut self, steps: usize) {
        for _ in 0..steps {
            dbg!(self.link_to_page.len());
            if steps == 0 {
                return;
            }
            // I could do A better breadthfirst by sorted based of distance
            // technically this clone might not be needed if we rearranged the values
            let links = self
                .link_to_page
                .values()
                .flat_map(|p| p.all_links.iter().cloned())
                .collect::<Vec<String>>();
            dbg!(links.len());
            for link in &links {
                self.add_link(link);
            }
        }
    }
}

fn main() {
    let file_path = "wikipedia_en_medicine_nopic_2025-09.zim";
    let mut wiki_graph = WikiGraph::new(file_path);
    let e = wiki_graph.a.get_randomentry().unwrap();
    dbg!(e.get_title());
    wiki_graph.add_link(&e.get_path());
    wiki_graph.expand(5);
    dbg!(wiki_graph
        .link_to_page
        .values()
        .flat_map(|p| p.all_links.iter())
        .count());
}
