#![feature(core)]
#![feature(hash)]
#![feature(collections)]
#![feature(dir_entry_ext)]
#![feature(path_relative_from)]
#![feature(associated_consts)]
#![feature(test)]
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate test;

// general TODO:
// - create our own error type and use that everywhere
// - unify error handling to be more descriptive (replace try!, unwrap)
// - move fileops into a separate module so we can mock it out for testing

use std::path::PathBuf;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::cmp::Ordering;
use std::hash::{hash, SipHasher};
use std::io::{BufReader, BufRead};

use std::fmt;
use std::fs;
use std::io;

use tree::*;

mod tree;

const INDEX_PLACES_SIZE: usize = 8;

#[derive(Debug)]
struct Stage {
    path: PathBuf
}

#[derive(Debug)]
struct Checkout {
    pub path: PathBuf
}

struct PathInfo {
    path: PathBuf,
    pub id: PathBuf,
    pub metadata: fs::Metadata
}

#[derive(Debug)]
struct Logs {
    path: PathBuf
}

#[derive(Debug, Clone, Copy)]
struct IndexNext {
    hash: u64,
    // idx of node in tree
    idx: u64
}

// TODO: Improve this structure to include more caching
struct IndexItem {
    hash: u64,
    order: usize,
    count: usize,
    places: [u64; INDEX_PLACES_SIZE]
}

impl fmt::Debug for IndexItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "IndexItem {{ hash: {:?}, order: {:?}, count: {:?}, places: [",
                    self.hash, self.order, self.count));
        if self.count > 0 {
            try!(write!(f, "{:?}", self.places[0]));
        }
        if self.count > 1 {
            for i in 1..self.count {
                try!(write!(f, ", {:?}", self.places[i]));
            }
        }
        write!(f, "] }}")
    }
}

impl Default for IndexItem {
    fn default() -> IndexItem {
        IndexItem {
            hash: 0,
            order: 0,
            count: 0,
            places: [0; INDEX_PLACES_SIZE]
        }
    }
}

impl Copy for IndexItem {}

impl Clone for IndexItem {
    fn clone(&self) -> IndexItem {
        *self
    }
}

impl Eq for IndexItem {}

impl PartialEq for IndexItem {
    fn eq(&self, other: &IndexItem) -> bool {
        self.hash == other.hash && self.count == other.count
    }
}

impl Ord for IndexItem {
    fn cmp(&self, other: &IndexItem) -> Ordering {
        if self.hash < other.hash {
            Ordering::Less
        } else if self.hash > other.hash {
            Ordering::Greater
        } else if self.count < other.count {
            Ordering::Less
        } else if self.count > other.count {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for IndexItem {
    fn partial_cmp(&self, other: &IndexItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for PathInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PathInfo {{ path: {:?}, id: {:?}, metadata: {{...}} }}", self.path, self.id)
    }
}

impl PathInfo {
    pub fn new<T: Into<PathBuf>, V: Into<PathBuf>>(path: T, id: V, metadata: fs::Metadata) -> PathInfo {
        PathInfo {
            path: path.into(),
            id: id.into(),
            metadata: metadata
        }
    }

    pub fn get_buffer(&self) -> Result<fs::File, io::Error> {
        fs::File::open(&self.path)
    }

    pub fn copy<T: Into<PathBuf>>(&self, to: T) -> Result<(), io::Error> {
        if self.metadata.is_dir() {
            trace!("Copying as directory");
            self.copy_dir(to)
        } else if self.metadata.is_file() {
            trace!("Copying as file");
            self.copy_file(to)
        } else {
            error!("{} is neither a file nor a directory", self.path.display());
            unimplemented!()
        }
    }

    fn copy_dir<T: Into<PathBuf>>(&self, to: T) -> Result<(), io::Error> {
        let dest_path = to.into().join(&self.id);
        debug!("Creating directory at {:?}", &dest_path);
        match fs::create_dir_all(dest_path) {
            Err(e) => {
                error!("Failed to create directory: {}", e);
                Err(e)
            },
            Ok(_) => {
                trace!("Directory created successfully");
                Ok(())
            }
        }
    }

    fn copy_file<T: Into<PathBuf>>(&self, to: T) -> Result<(), io::Error> {
        let dest_path = to.into().join(&self.id);

        debug!("Creating parent directory for path");
        match fs::create_dir_all(dest_path.parent().unwrap()) {
            Err(e) => {
                error!("Failed to create parent directory: {}", e);
                return Err(e);
            },
            Ok(_) => {
                trace!("Directory created");
            }
        }

        debug!("Copying {:?} to {:?}", &self.path, &dest_path);
        match fs::copy(&self.path, &dest_path) {
            Err(e) => {
                error!("Failed to copy {} to {}: {}", self.path.display(), dest_path.display(), e);
                Err(e)
            },
            Ok(_) => {
                trace!("Copy succeeded");
                Ok(())
            }
        }
    }
}

impl Default for Stage {
    fn default() -> Stage {
        Stage::new("./.h2/stage")
    }
}

impl Stage {
    pub fn new<T: Into<PathBuf>>(path: T) -> Stage {
        Stage {
            path: path.into(),
        }
    }

    pub fn init(&mut self) -> Result<(), io::Error> {
        info!("Creating Stage");
        match fs::create_dir_all(&self.path) {
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {
                trace!("Directory already existed");
                Ok(())
            },
            Err(e) => {
                error!("Failed to create directory \"{}\": {}", self.path.display(), e);
                Err(e)
            },
            Ok(_) => {
                trace!("Directory created");
                Ok(())
            }
        }
    }

    pub fn add_path(&mut self, path: &PathInfo) -> Result<(), io::Error> {
        // initial implementation. Overwrites anything.
        info!("Adding path {:?}", path);
        // copy the path to the stage
        path.copy(&self.path)
    }
}

impl Default for Checkout {
    fn default() -> Checkout {
        Checkout::new(".")
    }
}

impl Checkout {
    pub fn new<T: Into<PathBuf>>(path: T) -> Checkout {
        Checkout {
            path: path.into()
        }
    }

    pub fn init(&mut self) -> Result<(), io::Error> {
        info!("Creating checkout");
        match fs::create_dir_all(&self.path) {
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {
                trace!("Directory already existed");
                Ok(())
            },
            Err(e) => {
                error!("Failed to create directory \"{}\": {}", self.path.display(), e);
                Err(e)
            },
            Ok(_) => {
                trace!("Directory created");
                Ok(())
            }
        }
    }
}

impl Default for Logs {
    fn default() -> Logs {
        Logs::new("./.h2/logs")
    }
}

impl Logs {
    pub fn new<T: Into<PathBuf>>(path: T) -> Logs {
        Logs {
            path: path.into()
        }
    }

    pub fn init(&mut self) -> Result<(), io::Error> {
        info!("Creating logs");
        match fs::create_dir_all(&self.path) {
            Err(ref e) if e.kind() == io::ErrorKind::AlreadyExists => {
                trace!("Directory already existed");
                Ok(())
            },
            Err(e) => {
                error!("Failed to create directory \"{}\": {}", self.path.display(), e);
                Err(e)
            },
            Ok(_) => {
                trace!("Directory created");
                Ok(())
            }
        }
    }

    pub fn add_path(&mut self, path: &PathInfo) -> io::Result<()> {
        let dest_path = self.path.join(&path.id);
        if !path.metadata.is_file() {
            // only create an index for a file
            return Ok(());
        }

        debug!("Creating log directory");
        match fs::create_dir_all(&dest_path) {
            Err(e) => {
                error!("Failed to create parent directory: {}", e);
                return Err(e);
            },
            Ok(_) => {
                trace!("Parent directory created");
            }
        }

        debug!("Creating tree at {:?} from {:?}", &dest_path, path);

        trace!("Creating destination buffer");
        let dest = match fs::File::create(dest_path.join("content")) {
            Err(e) => {
                error!("Failed to create destination buffer: {}", e);
                return Err(e);
            },
            Ok(b) => {
                trace!("Successfully created destination buffer");
                b
            }
        };

        trace!("Creating tree object");
        let mut tree: BufTree<_, IndexItem> = match BufTree::new(dest, 10) {
            Err(e) => {
                error!("Failed to create tree: {}", e);
                return Err(e);
            },
            Ok(t) => {
                trace!("Successfully created tree");
                t
            }
        };

        trace!("Opening original file");
        let mut orig = match path.get_buffer() {
            Err(e) => {
                error!("Failed to open file: {}", e);
                return Err(e);
            },
            Ok(b) => {
                trace!("Successfully opened file");
                // wrap in a buffreader so we can read_line
                BufReader::new(b)
            }
        };

        debug!("Inserting original lines into tree");
        // TODO: Improve this algorithm
        let mut line = String::new();
        let mut idx = 0;
        loop {
            line.clear();
            match orig.read_line(&mut line) {
                Err(e) => {
                    error!("Error reading line: {}", e);
                    return Err(e);
                },
                Ok(count) => {
                    trace!("Got new line");
                    idx += count as u64;
                }
            };
            trace!("Creating initial item");
            let mut item = IndexItem {
                hash: hash::<_, SipHasher>(&line),
                order: 0,
                count: 1,
                places: [idx; INDEX_PLACES_SIZE],
            };
            trace!("Merging with tree");
            item = match tree.get(&item) {
                Err(e) => {
                    error!("Error getting item: {}", e);
                    return Err(e);
                },
                Ok(None) => {
                    trace!("No matching item");
                    item
                },
                Ok(Some(mut item)) => {
                    trace!("Found a matching item, merging");
                    while item.count == INDEX_PLACES_SIZE {
                        item.order += 1;
                        item = match tree.get(&item) {
                            Err(e) => {
                                error!("Error getting item: {}", e);
                                return Err(e);
                            },
                            Ok(None) => {
                                trace!("Creating new item");
                                item.count = 1;
                                item.places[0] = idx;
                                item
                            },
                            Ok(Some(mut item)) => {
                                trace!("Found follow-up item");
                                item.places[item.count] = idx;
                                item.count += 1;
                                item
                            }
                        }
                    }
                    item
                }
            };
            trace!("Inserting item {:?}", &item);
            match tree.insert(item) {
                Err(e) => {
                    error!("Error inserting item: {}", e);
                    return Err(e);
                },
                Ok(_) => {
                    trace!("Successfully inserted item");
                }
            }
        }
    }
}

fn main() {
    // start up logging
    match env_logger::init() {
        Ok(()) => {
            trace!("Logger initialization successful");
        },
        Err(e) => {
            panic!("Failed to start up logging: {}", e);
        }
    }

    info!("Init in current directory");
    match init() {
        Ok(()) => {
            trace!("Init successful");
        },
        Err(e) => {
            panic!("Init failed: {}", e);
        }
    }

    trace!("Creating checkout object");
    let mut checkout = Checkout::default();
    debug!("Initializing checkout");
    match checkout.init() {
        Ok(()) => {
            trace!("Checkout creation successful");
        },
        Err(e) => {
            panic!("Checkout creation failed: {}", e);
        }
    }
    
    trace!("Creating Stage object");
    let mut stage = Stage::default();
    debug!("Initializing stage");
    match stage.init() {
        Ok(()) => {
            trace!("Stage creation successful");
        },
        Err(e) => {
            panic!("Stage creation failed: {}", e);
        }
    }

    trace!("Creating Logs object");
    let mut logs = Logs::default();
    debug!("Initializing logs");
    match logs.init() {
        Ok(()) => {
            trace!("Logs creation successful");
        },
        Err(e) => {
            panic!("Logs creation failed: {}", e);
        }
    }
    
    info!("Walking current directory");
    match stage_dir_all(&checkout, &mut logs, &mut stage, PathBuf::from("."), vec![".h2"]) {
        Ok(()) => {
            debug!("Walk successful");
        },
        Err(e) => {
            panic!("Walk failed: {}", e);
        }
    }
}

fn init() -> Result<(), io::Error> {
    info!("Creating half2 directories");

    debug!("Creating ./.h2");
    match fs::create_dir("./.h2") {
        Err(e) => {
            error!("Failed to create directory \".h2\": {}", e);
            return Err(e);
        },
        Ok(_) => {
            trace!("Directory created");
        }
    }

    Ok(())
}

fn stage_dir_all<T: Into<PathBuf>, V: IntoIterator>(checkout: &Checkout, logs: &mut Logs, stage: &mut Stage, path: T, ignore: V)
                                                        -> Result<(), io::Error> where V::Item: Into<PathBuf> {
    let mut to_visit = vec![checkout.path.join(path.into())];
    let to_ignore: HashSet<PathBuf> = HashSet::from_iter(ignore.into_iter().map(|x| {x.into()}));

    info!("Copying directory tree");
    while !to_visit.is_empty() {
        trace!("Popping directory from queue");
        let dir = to_visit.pop().unwrap();
        debug!("Reading directory {:?}", dir);
        for item in match fs::read_dir(dir) {
            Ok(iter) => {
                trace!("Got directory iterator");
                iter
            },
            Err(e) => {
                error!("Failed to read directory: {}", e);
                return Err(e);
            }
        } {
            let entry = match item {
                Ok(item) => {
                    trace!("No new error");
                    item
                },
                Err(e) => {
                    error!("Error reading directory: {}", e);
                    return Err(e);
                }
            };

            trace!("Getting path relative to checkout directory");
            let id = match entry.path().relative_from(&checkout.path) {
                Some(id) => {
                    trace!("Got path relative_from successfully");
                    PathBuf::from(id)
                },
                None => {
                    panic!("Failed to get path relative to checkout path");
                }
            };

            trace!("Entry path: {:?}", entry.path());
            if to_ignore.contains(&id) {
                // ignore our own directory
                trace!("Path was in ignore set");
                continue;
            }

            trace!("Getting file metadata");
            let metadata = match entry.metadata() {
                Ok(data) => {
                    trace!("Got metadata");
                    data
                },
                Err(e) => {
                    error!("Could not get file metadata: {}", e);
                    return Err(e);
                }
            };

            if metadata.is_dir() {
                trace!("Adding path to visit queue");
                to_visit.push(entry.path());
            } else {
                trace!("Not adding path to visit queue");
            }
            
            trace!("Creating path info object");
            let info = PathInfo::new(entry.path(), id, metadata);

            debug!("Adding path to stage");
            match stage.add_path(&info) {
                Ok(()) => {
                    trace!("Add path succeeded");
                },
                Err(e) => {
                    error!("Add path failed: {}", e);
                    return Err(e);
                }
            }

            debug!("Creating file index");
            match logs.add_path(&info) {
                Ok(()) => {
                    trace!("Index creation successful");
                },
                Err(e) => {
                    error!("Index creation failed: {}", e);
                    return Err(e);
                }
            }
        }
    }

    trace!("Init finished");
    Ok(())
}
