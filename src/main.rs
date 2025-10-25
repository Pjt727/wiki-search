use std::collections::HashSet;
// Create a list of every new term
//
// 1. search out n times adding each term found
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

// Compression libraries
use flate2::read::ZlibDecoder;
use xz2::read::XzDecoder;

#[derive(Debug)]
pub enum ZimError {
    Io(io::Error),
    InvalidFormat(String),
    UnsupportedVersion(u16, u16),
    UnsupportedCompression(u8),
    InvalidEntry(String),
}

impl From<io::Error> for ZimError {
    fn from(err: io::Error) -> Self {
        ZimError::Io(err)
    }
}

impl std::fmt::Display for ZimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZimError::Io(e) => write!(f, "IO error: {}", e),
            ZimError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            ZimError::UnsupportedVersion(maj, min) => {
                write!(f, "Unsupported version: {}.{}", maj, min)
            }
            ZimError::UnsupportedCompression(c) => write!(f, "Unsupported compression: {}", c),
            ZimError::InvalidEntry(msg) => write!(f, "Invalid entry: {}", msg),
        }
    }
}

impl std::error::Error for ZimError {}

#[derive(Debug, Clone)]
pub struct ZimHeader {
    pub magic: u32,
    pub major_version: u16,
    pub minor_version: u16,
    pub uuid: [u8; 16],
    pub entry_count: u32,
    pub cluster_count: u32,
    pub url_ptr_pos: u64,
    pub title_ptr_pos: u64,
    pub cluster_ptr_pos: u64,
    pub mime_list_pos: u64,
    pub main_page: u32,
    pub layout_page: u32,
    pub checksum_pos: u64,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub mime_type: u16,
    pub namespace: char,
    pub cluster_number: u32,
    pub blob_number: u32,
    pub url: String,
    pub title: String,
    pub redirect_index: Option<u32>,
}

pub struct ZimReader {
    file: File,
    header: ZimHeader,
    mime_types: Vec<String>,
}

impl ZimReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, ZimError> {
        let mut file = File::open(path)?;
        let header = Self::read_header(&mut file)?;
        let mime_types = Self::read_mime_types(&mut file, &header)?;

        Ok(ZimReader {
            file,
            header,
            mime_types,
        })
    }

    fn read_u16(&mut self) -> Result<u16, ZimError> {
        let mut buf = [0u8; 2];
        self.file.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    fn read_u32(&mut self) -> Result<u32, ZimError> {
        let mut buf = [0u8; 4];
        self.file.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64(&mut self) -> Result<u64, ZimError> {
        let mut buf = [0u8; 8];
        self.file.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_header(file: &mut File) -> Result<ZimHeader, ZimError> {
        file.seek(SeekFrom::Start(0))?;

        let mut magic_buf = [0u8; 4];
        file.read_exact(&mut magic_buf)?;
        let magic = u32::from_le_bytes(magic_buf);

        if magic != 0x044D495A {
            // "ZIM\x04" in little-endian
            return Err(ZimError::InvalidFormat(format!(
                "Invalid magic number: 0x{:08X}",
                magic
            )));
        }

        let mut buf = [0u8; 2];
        file.read_exact(&mut buf)?;
        let major_version = u16::from_le_bytes(buf);

        file.read_exact(&mut buf)?;
        let minor_version = u16::from_le_bytes(buf);

        if major_version < 5 || major_version > 6 {
            return Err(ZimError::UnsupportedVersion(major_version, minor_version));
        }

        let mut uuid = [0u8; 16];
        file.read_exact(&mut uuid)?;

        let mut buf4 = [0u8; 4];
        file.read_exact(&mut buf4)?;
        let entry_count = u32::from_le_bytes(buf4);

        file.read_exact(&mut buf4)?;
        let cluster_count = u32::from_le_bytes(buf4);

        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8)?;
        let url_ptr_pos = u64::from_le_bytes(buf8);

        file.read_exact(&mut buf8)?;
        let title_ptr_pos = u64::from_le_bytes(buf8);

        file.read_exact(&mut buf8)?;
        let cluster_ptr_pos = u64::from_le_bytes(buf8);

        file.read_exact(&mut buf8)?;
        let mime_list_pos = u64::from_le_bytes(buf8);

        file.read_exact(&mut buf4)?;
        let main_page = u32::from_le_bytes(buf4);

        file.read_exact(&mut buf4)?;
        let layout_page = u32::from_le_bytes(buf4);

        file.read_exact(&mut buf8)?;
        let checksum_pos = u64::from_le_bytes(buf8);

        Ok(ZimHeader {
            magic,
            major_version,
            minor_version,
            uuid,
            entry_count,
            cluster_count,
            url_ptr_pos,
            title_ptr_pos,
            cluster_ptr_pos,
            mime_list_pos,
            main_page,
            layout_page,
            checksum_pos,
        })
    }

    fn read_mime_types(file: &mut File, header: &ZimHeader) -> Result<Vec<String>, ZimError> {
        file.seek(SeekFrom::Start(header.mime_list_pos))?;

        let mut mime_types = Vec::new();
        loop {
            let mut mime = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                file.read_exact(&mut byte)?;
                if byte[0] == 0 {
                    break;
                }
                mime.push(byte[0]);
            }

            if mime.is_empty() {
                break;
            }

            mime_types.push(String::from_utf8_lossy(&mime).into_owned());
        }

        Ok(mime_types)
    }

    fn read_dir_entry_at(&mut self, offset: u64) -> Result<DirEntry, ZimError> {
        self.file.seek(SeekFrom::Start(offset))?;

        let mime_type = self.read_u16()?;

        let mut param_len = 0u8;
        self.file.read_exact(std::slice::from_mut(&mut param_len))?;

        let mut namespace_byte = [0u8; 1];
        self.file.read_exact(&mut namespace_byte)?;
        let namespace = namespace_byte[0] as char;

        let _revision = self.read_u32()?;

        let is_redirect = mime_type == 0xFFFF;

        let (cluster_number, blob_number, redirect_index) = if is_redirect {
            let redir_idx = self.read_u32()?;
            (0, 0, Some(redir_idx))
        } else {
            let cluster = self.read_u32()?;
            let blob = self.read_u32()?;
            (cluster, blob, None)
        };

        let mut url_bytes = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.file.read_exact(&mut byte)?;
            if byte[0] == 0 {
                break;
            }
            url_bytes.push(byte[0]);
        }
        let url = String::from_utf8_lossy(&url_bytes).into_owned();

        let mut title_bytes = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.file.read_exact(&mut byte)?;
            if byte[0] == 0 {
                break;
            }
            title_bytes.push(byte[0]);
        }
        let title = if title_bytes.is_empty() {
            url.clone()
        } else {
            String::from_utf8_lossy(&title_bytes).into_owned()
        };

        // Skip parameter data
        if param_len > 0 {
            let mut params = vec![0u8; param_len as usize];
            self.file.read_exact(&mut params)?;
        }

        Ok(DirEntry {
            mime_type,
            namespace,
            cluster_number,
            blob_number,
            url,
            title,
            redirect_index,
        })
    }

    pub fn list_articles(&mut self) -> Vec<DirEntry> {
        let mut articles = Vec::new();

        for i in 0..self.header.entry_count {
            match self.get_entry_by_index(i) {
                Ok(entry) => {
                    if entry.namespace == 'A' || entry.namespace == 'C' {
                        articles.push(entry);
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Failed to read entry {}: {}", i, e);
                }
            }
        }
        articles.sort_by(|a, b| a.title.cmp(&b.title));

        articles
    }

    fn get_entry_by_index(&mut self, index: u32) -> Result<DirEntry, ZimError> {
        if index >= self.header.entry_count {
            return Err(ZimError::InvalidEntry(format!(
                "Index {} out of range",
                index
            )));
        }

        let ptr_pos = self.header.url_ptr_pos + (index as u64 * 8);
        self.file.seek(SeekFrom::Start(ptr_pos))?;
        let entry_offset = self.read_u64()?;

        self.read_dir_entry_at(entry_offset)
    }

    pub fn find_by_title(&mut self, title: &str) -> Option<DirEntry> {
        // Binary search through title pointer list
        let mut low = 0u32;
        // let mut high = self.header.entry_count;
        let mut high = 0xFFFFFFFFu32;

        let mut unique = HashSet::new();

        for i in low..high {
            if let Ok(dir_entry) = self.get_entry_by_title_index(i) {
                let did_add = unique.insert(dir_entry.title.to_string());
                if did_add && i % 100 == 0 {
                    dbg!(i, dir_entry.title.to_string());
                }
                // if dir_entry.title.contains(title) {
                //     return Some(dir_entry);
                // }
            }
        }
        dbg!(unique.len());
        None
    }

    fn get_entry_by_title_index(&mut self, title_idx: u32) -> Result<DirEntry, ZimError> {
        // if title_idx >= self.header.entry_count {
        //     return Err(ZimError::InvalidEntry(format!(
        //         "Title index {} out of range",
        //         title_idx
        //     )));
        // }

        let ptr_pos = self.header.title_ptr_pos + (title_idx as u64 * 4);
        self.file.seek(SeekFrom::Start(ptr_pos))?;
        let url_index = self.read_u32()?;

        self.get_entry_by_index(url_index)
    }

    pub fn read_blob(&mut self, entry: &DirEntry) -> Result<Vec<u8>, ZimError> {
        if entry.redirect_index.is_some() {
            return Err(ZimError::InvalidEntry(
                "Cannot read blob of redirect".into(),
            ));
        }

        let cluster_ptr_pos = self.header.cluster_ptr_pos + (entry.cluster_number as u64 * 8);
        self.file.seek(SeekFrom::Start(cluster_ptr_pos))?;
        let cluster_offset = self.read_u64()?;

        self.file.seek(SeekFrom::Start(cluster_offset))?;
        let mut compression_byte = [0u8; 1];
        self.file.read_exact(&mut compression_byte)?;

        let compression = compression_byte[0] & 0x0F;
        let extended = (compression_byte[0] & 0x10) != 0;

        let offset_size = if extended { 8 } else { 4 };

        // Read first offset to determine number of blobs
        let first_offset = if offset_size == 8 {
            self.read_u64()? as usize
        } else {
            self.read_u32()? as usize
        };

        let num_offsets = first_offset / offset_size;
        let mut offsets = vec![first_offset];

        for _ in 1..num_offsets {
            let offset = if offset_size == 8 {
                self.read_u64()? as usize
            } else {
                self.read_u32()? as usize
            };
            offsets.push(offset);
        }

        if entry.blob_number as usize >= offsets.len() - 1 {
            return Err(ZimError::InvalidEntry(format!(
                "Blob {} not found in cluster",
                entry.blob_number
            )));
        }

        let blob_start = offsets[entry.blob_number as usize];
        let blob_end = offsets[entry.blob_number as usize + 1];
        let blob_size = blob_end - blob_start;

        // Read compressed cluster data
        let cluster_data_start = cluster_offset + 1 + (num_offsets * offset_size) as u64;
        self.file.seek(SeekFrom::Start(cluster_data_start))?;

        let mut compressed_data = Vec::new();
        self.file.read_to_end(&mut compressed_data)?;

        let decompressed = match compression {
            0 | 1 => compressed_data, // No compression
            4 => {
                // LZMA2 / XZ
                let mut decoder = XzDecoder::new(&compressed_data[..]);
                let mut result = Vec::new();
                decoder.read_to_end(&mut result)?;
                result
            }
            5 => {
                // Zstandard - would need zstd crate
                return Err(ZimError::UnsupportedCompression(5));
            }
            _ => return Err(ZimError::UnsupportedCompression(compression)),
        };

        if blob_start > decompressed.len() || blob_end > decompressed.len() {
            return Err(ZimError::InvalidEntry(
                "Blob offset out of decompressed data range".into(),
            ));
        }

        Ok(decompressed[blob_start..blob_end].to_vec())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <zim_file> [title_to_search]", args[0]);
        std::process::exit(1);
    }

    let zim_path = &args[1];
    let mut reader = ZimReader::new(zim_path)?;

    println!("ZIM file opened successfully");
    println!(
        "Version: {}.{}",
        reader.header.major_version, reader.header.minor_version
    );
    println!("Entries: {}", reader.header.entry_count);
    println!("Clusters: {}", reader.header.cluster_count);
    println!("MIME types: {}", reader.mime_types.len());
    println!();

    if args.len() >= 3 {
        let search_title = &args[2];
        println!("Searching for title: {}", search_title);

        match reader.find_by_title(search_title) {
            Some(entry) => {
                println!("Found: {:?}", entry);

                if entry.redirect_index.is_none() {
                    match reader.read_blob(&entry) {
                        Ok(data) => {
                            println!("Blob size: {} bytes", data.len());
                            if let Ok(text) = String::from_utf8(data.clone()) {
                                println!("Content preview (first 500 chars):");
                                println!("{}", &text.chars().take(500).collect::<String>());
                            }
                        }
                        Err(e) => eprintln!("Failed to read blob: {}", e),
                    }
                }
            }
            None => println!("Title not found"),
        }
    } else {
        println!("Listing articles (first 50):");
        let articles = reader.list_articles();

        for (i, entry) in articles.iter().take(50).enumerate() {
            println!(
                "{}: [{}] {} (title: {})",
                i + 1,
                entry.namespace,
                entry.url,
                entry.title
            );
        }

        println!("\nTotal articles found: {}", articles.len());
    }

    Ok(())
}
