use std::collections::{HashMap, HashSet};

use scraper::{Html, Selector};
use zim_rs::archive::Archive;
use zim_rs::entry::Entry;
use zim_rs::search::{Query, Searcher};

struct Page {
    links: Vec<(f32, String)>,
    links_set: HashSet<String>,
}

struct WikiGraph {
    link_to_page: HashMap<String, Page>,
}

fn main() {
    let file_path = "wikipedia_en_medicine_nopic_2025-09.zim";
    let a = Archive::new(file_path).unwrap();
    // println!("{}", r.get_title());
    println!("{}", a.get_entrycount());
    let count = 1000;
    let mut num_dup = 0;
    let mut num_empty = 0;
    for _ in 0..count {
        let r = a.get_randomentry().unwrap();
        let page = get_page(r, &a);
        if let Some(p) = page {
            num_dup += p.links.len() - p.links_set.len();
        } else {
            num_empty += 1;
        }
    }
    dbg!(num_dup, num_empty, count);
    let average = num_dup / count;
    println!("Average link count: {average}")
    // println!("Count: {}", counts)

    // let i = r.get_item(true).unwrap();
    // let blob = i.get_data().unwrap();
    // let d = blob.data();
    // println!("{}", String::from_utf8(d.to_vec()).unwrap());
    //
    // let e = a
    //     .get_entry_bypath_str("COVID-19_pandemic_in_the_United_Kingdom")
    //     .unwrap();
    // println!("{}", e.get_title());

    // let mut sr = Searcher::new(&a).unwrap();
    // let q = Query::new("Blood").unwrap();
    // let s = sr.search(&q).unwrap();
    // let result_set = s.get_results(0, 1).unwrap();
    // for res in result_set {
    //     let r = res.unwrap();
    //     println!("{}", r.get_title())
    // }
}

fn get_page(e: Entry, a: &Archive) -> Option<Page> {
    let i = e.get_item(true).unwrap();
    let blob = i.get_data().unwrap();
    let d = blob.data();
    let doc = match String::from_utf8(d.to_vec()) {
        Ok(s) => Html::parse_document(&s),
        Err(_) => return None,
    };
    let selector = Selector::parse("a[href]").unwrap();
    let mut page = Page {
        links: vec![],
        links_set: HashSet::new(),
    };
    doc.select(&selector)
        .filter_map(|e| a.get_entry_bypath_str(e.attr("href")?).ok())
        .enumerate()
        .for_each(|(i, e)| {
            let title = e.get_title().to_string();
            page.links.push((i as f32, title.clone()));
            page.links_set.insert(title);
        });
    Some(page)
}
