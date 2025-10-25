// src/lib.rs
//! Minimal ZIM reader (single-file) using only memmap2, byteorder and xz2.
//!
//! - parse header
//! - read title pointer list
//! - minimal dirent parsing (path, title, mimetype, cluster, blob)
//! - get article HTML by decompressing cluster with xz2
//!
//! NOTE: This is a minimal reader and deliberately doesn't implement the
//! entire ZIM specification. See README comments below.

use std::fs::File;
use std::io::{self, Cursor, Read};
use std::path::Path;
use std::str;
use std::sync::Arc;

use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::Mmap;
use thiserror::Error;
use xz2::read::XzDecoder;

#[derive(Error, Debug)]
pub enum ZimError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid magic")]
    InvalidMagic,
    #[error("unsupported / unexpected format")]
    Unsupported,
    #[error("decompression failed: {0}")]
    Decompress(String),
    #[error("entry parse error")]
    EntryParse,
}

/// Minimal header fields we need
#[derive(Debug)]
pub struct ZimHeader {
    pub major_version: u16,
    pub minor_version: u16,
    pub uuid: [u8; 16],
    pub article_count: u32,
    pub cluster_count: u32,
    pub url_ptr_pos: u64,
    pub title_ptr_pos: u64,
    pub cluster_ptr_pos: u64,
    pub mime_list_pos: u64,
    pub main_page: u32,
    pub layout_page: u32,
}

/// Light-weight directory entry we extract
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// path / URL-like path (UTF-8)
    pub path: String,
    /// human title (UTF-8)
    pub title: String,
    /// mimetype id (index into mimetype list)
    pub mimetype: u16,
    /// cluster number containing the blob
    pub cluster: u32,
    /// blob index inside the cluster
    pub blob_index: u32,
    /// raw offset where this dir entry lives (useful for debugging)
    pub offset: u64,
}

/// Internal: pointer into the dir table
#[derive(Debug)]
struct DirEntryIndex {
    offset: u64,
}

pub struct ZimReader {
    mmap: Arc<Mmap>,
    header: ZimHeader,
    title_index: Vec<DirEntryIndex>,
    cluster_ptrs: Vec<u64>,
    // mime list and other metadata could be parsed if needed
}

impl ZimReader {
    /// Open a ZIM file and parse header + title pointer list + cluster pointer list.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, ZimError> {
        let f = File::open(path)?;
        let mmap = unsafe { Mmap::map(&f)? };
        let arc_mmap = Arc::new(mmap);

        // parse header from first bytes
        let header = parse_header(&arc_mmap)?;

        // read URL/title pointer lists and cluster pointer list
        let title_index = read_ptr_list(&arc_mmap, header.title_ptr_pos, header.cluster_ptr_pos)?;
        let cluster_ptrs =
            read_ptr_list_u64(&arc_mmap, header.cluster_ptr_pos, header.mime_list_pos)?;

        Ok(Self {
            mmap: arc_mmap,
            header,
            title_index,
            cluster_ptrs,
        })
    }

    /// Return the number of titles we know about (should equal article_count in header)
    pub fn article_count(&self) -> usize {
        self.title_index.len()
    }

    /// Find an article by exact title (case-sensitive). Returns the parsed DirEntry.
    pub fn find_article_by_title(&self, title: &str) -> Result<Option<DirEntry>, ZimError> {
        // brute force scan titles (could be optimized to binary search if title index is sorted)
        for idx in &self.title_index {
            if let Ok(entry) = parse_dir_entry_minimal(&self.mmap, idx.offset) {
                if entry.title == title {
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }

    /// Get HTML (UTF-8) for a given DirEntry.
    ///
    /// This will:
    ///  - find cluster offset from cluster_ptrs[entry.cluster]
    ///  - read compressed cluster bytes up to next cluster offset
    ///  - decompress the cluster (assumes xz compression)
    ///  - parse the cluster's blob offsets table and return the chosen blob bytes as UTF-8 string
    pub fn get_article_html(&self, entry: &DirEntry) -> Result<String, ZimError> {
        // find cluster pointer offsets
        let cluster_no = entry.cluster as usize;
        if cluster_no >= self.cluster_ptrs.len() {
            return Err(ZimError::EntryParse);
        }
        let cluster_start = self.cluster_ptrs[cluster_no] as usize;
        let cluster_end = if cluster_no + 1 < self.cluster_ptrs.len() {
            self.cluster_ptrs[cluster_no + 1] as usize
        } else {
            // until end (or checksum area)
            self.mmap.len()
        };

        if cluster_start >= cluster_end || cluster_end > self.mmap.len() {
            return Err(ZimError::EntryParse);
        }

        // read compressed cluster bytes
        let comp = &self.mmap[cluster_start..cluster_end];

        // Many ZIMs use XZ / LZMA2 for cluster compression. We try to decompress with xz2.
        let mut dec = XzDecoder::new(Cursor::new(comp));
        let mut decompressed = Vec::new();
        dec.read_to_end(&mut decompressed)
            .map_err(|e| ZimError::Decompress(e.to_string()))?;

        // cluster format: first is a blob pointer list (u32 count followed by u32 offsets)
        // this minimal parsing assumes the cluster contains:
        //   u32 blob_count
        //   blob_count x u32 offsets (relative to start of decompressed cluster)
        // followed by blob data concatenated.
        let mut cur = Cursor::new(&decompressed);
        let blob_count = cur
            .read_u32::<LittleEndian>()
            .map_err(|_| ZimError::EntryParse)?;
        let mut offsets = Vec::with_capacity(blob_count as usize + 1);
        for _ in 0..blob_count {
            let off = cur
                .read_u32::<LittleEndian>()
                .map_err(|_| ZimError::EntryParse)?;
            offsets.push(off as usize);
        }
        // add end-of-cluster offset
        offsets.push(decompressed.len());

        let blob_idx = entry.blob_index as usize;
        if blob_idx >= (offsets.len() - 1) {
            return Err(ZimError::EntryParse);
        }
        let start = offsets[blob_idx];
        let end = offsets[blob_idx + 1];
        if start >= end || end > decompressed.len() {
            return Err(ZimError::EntryParse);
        }

        let blob = &decompressed[start..end];
        // interpret as UTF-8 HTML/text
        let s = String::from_utf8(blob.to_vec())?;
        Ok(s)
    }
}

/// Parse ZIM header (minimal fields). Uses the canonical offsets from the ZIM spec.
/// Referenced spec: docs.fileformat.com and zim crate docs. :contentReference[oaicite:1]{index=1}
fn parse_header(mmap: &Mmap) -> Result<ZimHeader, ZimError> {
    if mmap.len() < 72 {
        return Err(ZimError::Unsupported);
    }
    let mut cur = Cursor::new(&mmap[..]);

    // magic (LE)
    let magic = cur.read_u32::<LittleEndian>()?;
    const MAGIC_LE: u32 = 0x004D495A;
    const MAGIC_BE: u32 = 0x5A494D00;
    const MAGIC_V6: u32 = 0x044D495A;
    if magic != MAGIC_LE && magic != MAGIC_BE && magic != MAGIC_V6 {
        return Err(ZimError::InvalidMagic);
    }

    let major = cur.read_u16::<LittleEndian>()?;
    let minor = cur.read_u16::<LittleEndian>()?;
    let mut uuid = [0u8; 16];
    cur.read_exact(&mut uuid)?;

    let article_count = cur.read_u32::<LittleEndian>()?;
    let cluster_count = cur.read_u32::<LittleEndian>()?;
    let url_ptr_pos = cur.read_u64::<LittleEndian>()?;
    let title_ptr_pos = cur.read_u64::<LittleEndian>()?;
    let cluster_ptr_pos = cur.read_u64::<LittleEndian>()?;
    let mime_list_pos = cur.read_u64::<LittleEndian>()?;
    let main_page = cur.read_u32::<LittleEndian>()?;
    let layout_page = cur.read_u32::<LittleEndian>()?;

    Ok(ZimHeader {
        major_version: major,
        minor_version: minor,
        uuid,
        article_count,
        cluster_count,
        url_ptr_pos,
        title_ptr_pos,
        cluster_ptr_pos,
        mime_list_pos,
        main_page,
        layout_page,
    })
}

/// Read a pointer list of u64s between start_pos and end_pos (exclusive).
fn read_ptr_list(
    mmap: &Mmap,
    start_pos: u64,
    end_pos: u64,
) -> Result<Vec<DirEntryIndex>, ZimError> {
    if start_pos as usize >= mmap.len() {
        return Ok(Vec::new());
    }
    let end = std::cmp::min(end_pos as usize, mmap.len());
    let start = start_pos as usize;
    if start >= end {
        return Ok(Vec::new());
    }
    let slice = &mmap[start..end];
    let mut cur = Cursor::new(slice);
    let mut res = Vec::new();
    while (cur.position() as usize) + 8 <= slice.len() {
        let off = cur
            .read_u64::<LittleEndian>()
            .map_err(|_| ZimError::Unsupported)?;
        res.push(DirEntryIndex { offset: off });
    }
    Ok(res)
}

/// Read cluster pointer list (u64s) between start_pos and end_pos.
fn read_ptr_list_u64(mmap: &Mmap, start_pos: u64, end_pos: u64) -> Result<Vec<u64>, ZimError> {
    if start_pos as usize >= mmap.len() {
        return Ok(Vec::new());
    }
    let end = std::cmp::min(end_pos as usize, mmap.len());
    let start = start_pos as usize;
    if start >= end {
        return Ok(Vec::new());
    }
    let slice = &mmap[start..end];
    let mut cur = Cursor::new(slice);
    let mut res = Vec::new();
    while (cur.position() as usize) + 8 <= slice.len() {
        let off = cur
            .read_u64::<LittleEndian>()
            .map_err(|_| ZimError::Unsupported)?;
        res.push(off);
    }
    Ok(res)
}

/// Minimal parse for a directory entry at `offset` in file.
///
/// NOTE: this function implements a simple interpretation:
/// [path (NUL-terminated UTF-8)] [title (NUL-terminated UTF-8)]
/// [u16 mimetype] [u32 cluster] [u32 blob_index]
///
/// Many ZIMs conform to a layout compatible with this, but real-world
/// files can have a lot of extra features (redirects, hints, extra data).
fn parse_dir_entry_minimal(mmap: &Mmap, offset: u64) -> Result<DirEntry, ZimError> {
    let off = offset as usize;
    if off >= mmap.len() {
        return Err(ZimError::EntryParse);
    }
    let slice = &mmap[off..];

    // helper to read NUL-terminated string
    fn read_nul_string(slice: &[u8], pos: &mut usize) -> Result<String, ZimError> {
        let mut end = *pos;
        while end < slice.len() && slice[end] != 0 {
            end += 1;
        }
        if end >= slice.len() {
            return Err(ZimError::EntryParse);
        }
        let bytes = &slice[*pos..end];
        *pos = end + 1; // skip NUL
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    let mut p = 0usize;
    let path = read_nul_string(slice, &mut p)?;
    let title = read_nul_string(slice, &mut p)?;

    // ensure we have at least 2 + 4 + 4 bytes remaining for mimetype/cluster/blob
    if p + 2 + 4 + 4 > slice.len() {
        return Err(ZimError::EntryParse);
    }
    let mut cur = Cursor::new(&slice[p..]);
    let mimetype = cur
        .read_u16::<LittleEndian>()
        .map_err(|_| ZimError::EntryParse)?;
    let cluster = cur
        .read_u32::<LittleEndian>()
        .map_err(|_| ZimError::EntryParse)?;
    let blob_index = cur
        .read_u32::<LittleEndian>()
        .map_err(|_| ZimError::EntryParse)?;

    Ok(DirEntry {
        path,
        title,
        mimetype,
        cluster,
        blob_index,
        offset,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Note: these tests are placeholders. Testing requires a real .zim test file.
    #[test]
    fn open_nonexistent() {
        let r = ZimReader::open("/this/path/does/not/exist.zim");
        assert!(r.is_err());
    }
}
