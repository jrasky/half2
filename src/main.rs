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
extern crate rustc_serialize;

// general TODO:
// - create our own error type and use that everywhere
// - unify error handling to be more descriptive (replace try!, unwrap)
// - move fileops into a separate module so we can mock it out for testing

use std::path::PathBuf;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::cmp::Ordering;
use std::hash::{hash, SipHasher};
use std::io::{BufReader, BufRead, Read, Write};

use rustc_serialize::json;

use std::fmt;
use std::fs;
use std::io;
use std::mem;
use std::env;

use tree::*;

mod tree;

const INDEX_PLACES_SIZE: usize = 4;
const FILE_TREE_WIDTH: usize = 6;
const FILE_BLOCK_LENGTH: usize = 1;

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
struct IndexPlace {
    node: usize,
    offset: isize
}

// TODO: Improve this structure to include more caching
struct IndexItem {
    hash: u64,
    order: usize,
    count: usize,
    places: [IndexPlace; INDEX_PLACES_SIZE]
}

#[derive(RustcDecodable, RustcEncodable)]
struct FileMeta {
    node_count: usize
}

impl fmt::Debug for IndexItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "IndexItem {{ hash: {:?}, order: {:?}, count: {:?}, places: [",
                    self.hash, self.order, self.count));
        if self.count > 0 {
            try!(write!(f, "{:?}", self.places[0]));
        }
        if self.count > 1 {
            for i in 1..self.count as usize {
                try!(write!(f, ", {:?}", self.places[i]));
            }
        }
        write!(f, "] }}")
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
        self.hash == other.hash && self.order == other.order
    }
}

impl Ord for IndexItem {
    fn cmp(&self, other: &IndexItem) -> Ordering {
        if self.hash < other.hash {
            Ordering::Less
        } else if self.hash > other.hash {
            Ordering::Greater
        } else if self.order < other.order {
            Ordering::Less
        } else if self.order > other.order {
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

    pub fn diff_path(&self, path: &PathInfo) -> io::Result<()> {
        let dest_path = self.path.join(&path.id);
        if !path.metadata.is_file() {
            // only diff files and then a change
            error!("Path was not a file: {:?}", path);
            return Ok(());
        } else {
            info!("Diffing file: {:?}", path);
        }

        debug!("Reading tree at {:?} for file {:?}", &dest_path, path);

        trace!("Opening meta info file");
        let mut meta_buf = match fs::OpenOptions::new().read(true).write(false).open(dest_path.join("meta")) {
            Err(e) => {
                error!("Failed to open meta file: {}", e);
                return Err(e);
            },
            Ok(b) => {
                trace!("Successfully opened meta file");
                b
            }
        };

        let mut meta_str = String::new();
        trace!("Reading metadata file");
        match meta_buf.read_to_string(&mut meta_str) {
            Err(e) => {
                error!("Failed to read meta info: {}", e);
                return Err(e);
            },
            Ok(s) => {
                trace!("Successfully read meta file");
                s
            }
        };

        trace!("Decoding object");
        let mut meta: FileMeta = match json::decode(meta_str.as_ref()) {
            Err(e) => {
                panic!("Failed to decode meta object: {}", e);
            },
            Ok(obj) => {
                trace!("Successfully decoded meta object");
                obj
            }
        };

        trace!("Opening tree file");
        let tree_buf = match fs::File::open(dest_path.join("content")) {
            Err(e) => {
                error!("Failed to open content buffer: {}", e);
                return Err(e);
            },
            Ok(b) => {
                trace!("Opened tree file");
                b
            }
        };

        trace!("Creating tree object");

        let mut tree: BufTree<_, IndexItem> = match unsafe {BufTree::from_buffer(tree_buf)} {
            Err(e) => {
                error!("Failed to create tree object: {}", e);
                return Err(e);
            },
            Ok(t) => {
                trace!("Tree object created successfully");
                t
            }
        };

        debug!("Opening original file");
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

        debug!("Comparing lines");
        let mut offset: isize = 0;
        let mut new_offset: isize = 0;
        let mut counter = 0;
        let mut line = Vec::new();
        loop {
            unsafe {line.set_len(0)};
            trace!("Reading line");
            match orig.read_until(b'\n', &mut line) {
                Ok(0) => {
                    trace!("Done with this file");
                    break;
                },
                Ok(_) => {
                    trace!("Got new line: {:?}", String::from_utf8_lossy(&line));
                },
                Err(e) => {
                    error!("Failed to read line: {}", e);
                    return Err(e);
                }
            }
            trace!("Creating initial item");
            debug!("Counter {}: {:?}", counter, String::from_utf8_lossy(&line));
            let mut item = IndexItem {
                hash: hash::<_, SipHasher>(&line),
                order: 0,
                count: 0,
                places: unsafe {mem::zeroed()}
            };
            trace!("Searching in tree");
            match tree.get(&item) {
                Err(e) => {
                    error!("Failed to get item: {}", e);
                    return Err(e);
                },
                Ok(None) => {
                    info!("New node {}: {:?}", meta.node_count, String::from_utf8_lossy(&line));
                    if offset != meta.node_count as isize - counter as isize {
                        info!("Counter {}: offset {}", (counter - 1),
                              meta.node_count as isize - counter as isize - offset);
                        new_offset += meta.node_count as isize - counter as isize - offset;
                        offset = meta.node_count as isize - counter as isize;
                    }
                    meta.node_count += 1;
                },
                Ok(Some(tree_item)) => {
                    trace!("Found existing item: {:?}", &tree_item);
                    // iterate through the places we have
                    let mut next = None;
                    let mut place = tree_item.places[0];
                    let mut diff = new_offset + tree_item.places[0].node as isize - counter as isize - offset;
                    debug!("Starting place: {:?}", place);
                    debug!("Starting difference: {}", diff);
                    for i in 0..tree_item.count {
                        debug!("Considering place {:?}", tree_item.places[i]);
                        if counter as isize + offset + tree_item.places[i].offset == tree_item.places[i].node as isize {
                            // we've foun a match
                            next = Some(tree_item.places[i]);
                            debug!("Found a match: {:?}", &tree_item.places[i]);
                            break;
                        } else if (new_offset + tree_item.places[i].node as isize -
                                   counter as isize - offset).abs() < diff.abs() {
                            diff = new_offset + tree_item.places[i].node as isize -
                                counter as isize - offset;
                            place = tree_item.places[i];
                            debug!("offset {} new_offset {} place.offset {} place.node {}", offset, new_offset, place.offset, place.node);
                            debug!("Found a better solution {}: {:?}", diff, place);
                        }
                    }

                    // iterate through the next ones if they exist
                    if next.is_none() {
                        trace!("Checking for sub-items");
                    }
                    while next.is_none() {
                        item.order += 1;
                        match tree.get(&item) {
                            Err(e) => {
                                error!("Failed to get item: {}", e);
                                return Err(e);
                            },
                            Ok(None) => {
                                trace!("Iterated through all sub-items");
                                break;
                            },
                            Ok(Some(other_item)) => {
                                trace!("Found other sub-item: {:?}", &other_item);
                                for i in 0..other_item.count {
                                    debug!("Considering place {:?}", other_item.places[i]);
                                    if counter as isize + offset + other_item.places[i].offset == other_item.places[i].node as isize {
                                        // we've foun a match
                                        next = Some(other_item.places[i]);
                                        debug!("Found a match: {:?}", &other_item.places[i]);
                                        break;
                                    } else if (new_offset + other_item.places[i].node as isize -
                                               counter as isize - offset).abs() < diff.abs() {
                                        diff = new_offset + other_item.places[i].node as isize -
                                            counter as isize - offset;
                                        place = tree_item.places[i];
                                        debug!("offset {} new_offset {} place.offset {} place.node {}", offset, new_offset, place.offset, place.node);
                                        debug!("Found a better solution {}: {:?}", diff, place);
                                    }
                                }
                            }
                        }
                    }

                    trace!("Finalizing decision");
                    match next {
                        Some(place) => {
                            // our best path doesn't need an offset
                            trace!("Found matching place");
                            offset += place.offset;
                        },
                        None => {
                            // new next element
                            trace!("No matching place, creating new one");
                            debug!("Closest place: {:?}", place);
                            info!("Counter {}: offset {}", (counter - 1),
                                  place.node as isize - counter as isize - offset);
                            new_offset += place.node as isize - counter as isize - offset;
                            offset = place.node as isize - counter as isize;
                        }
                    }
                }
            }

            trace!("Incrementing counter");
            counter += 1;
        }

        // TODO: actually change the tree to match, write out info
        Ok(())
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

        trace!("Creating meta file");
        let mut meta = match fs::File::create(dest_path.join("meta")) {
            Err(e) => {
                error!("Failed to create meta buffer: {}", e);
                return Err(e);
            },
            Ok(b) => {
                trace!("Successfully created meta buffer");
                b
            }
        };

        trace!("Creating destination buffer");
        let dest = match fs::OpenOptions::new().read(true).write(true).create(true).open(dest_path.join("content")) {
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
        let mut tree: BufTree<_, IndexItem> = match BufTree::new(dest, FILE_TREE_WIDTH) {
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
        let mut line = Vec::new();
        let mut counter = 0;
        let mut item;
        loop {
            unsafe {line.set_len(0)};
            trace!("Reading line");
            match orig.read_until(b'\n', &mut line) {
                Ok(0) => {
                    trace!("Done with this file");
                    break;
                },
                Ok(_) => {
                    trace!("Got new line: {:?}", String::from_utf8_lossy(&line));
                },
                Err(e) => {
                    error!("Failed to read line: {}", e);
                    return Err(e);
                }
            }
            trace!("Creating initial item");
            item = IndexItem {
                hash: hash::<_, SipHasher>(&line),
                order: 0,
                count: 0,
                // create zeroed memory so it compresses better
                places: unsafe {mem::zeroed()}
            };
            trace!("Merging with tree");
            loop {
                match tree.get(&item) {
                    Err(e) => {
                        error!("Failed to get tree item: {}", e);
                        return Err(e);
                    },
                    Ok(None) => {
                        trace!("Creating new tree item");
                        break;
                    },
                    Ok(Some(tree_item)) => {
                        if tree_item.count >= INDEX_PLACES_SIZE {
                            trace!("Found full item, incrementing");
                            item.order += 1;
                        } else {
                            trace!("Found item with space, merging");
                            item = tree_item;
                            break;
                        }
                    }
                }
            }
            trace!("Inserting element");
            item.places[item.count] = IndexPlace {
                node: counter,
                offset: 0
            };
            item.count += 1;
            debug!("Counter {}: {:?}", counter, String::from_utf8_lossy(&line));
            trace!("Inserting item into tree");
            match tree.insert(item) {
                Ok(_) => {
                    trace!("Inserted element successfully");
                },
                Err(e) => {
                    error!("Failed to insert element: {}", e);
                    return Err(e);
                }
            }
            trace!("Incrementing counter");
            counter += 1;
        }
        trace!("Finished inserting lines");

        debug!("Saving meta info");
        trace!("Creating meta object");
        let meta_info = FileMeta {
            node_count: counter
        };
        trace!("Creating json");
        let data = match json::encode(&meta_info) {
            Err(e) => {
                panic!("Failed to encode to json: {}", e)
            },
            Ok(d) => {
                trace!("Data encoded successfully");
                d
            }
        };
        trace!("Writing to file");
        match meta.write_all(data.as_ref()) {
            Err(e) => {
                error!("Failed to write meta info to file: {}", e);
                return Err(e);
            },
            Ok(()) => {
                trace!("Meta info written to file successfully");
            }
        }
        Ok(())
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

    trace!("Getting command-line arguments");
    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "init" {
        info!("Init in current directory");
        match init() {
            Ok(()) => {
                trace!("Init successful");
            },
            Err(e) => {
                panic!("Init failed: {}", e);
            }
        }
    } else {
        let checkout = Checkout::default();
        //let stage = Stage::default();
        let logs = Logs::default();

        info!("Walking current directory");
        match diff_dir_all(&checkout, &logs, PathBuf::from("."), vec![".h2", ".git", "target", "perf.data", "src"]) {
            Ok(()) => {
                debug!("Walk successful");
            },
            Err(e) => {
                panic!("Walk failed: {}", e);
            }
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

    trace!("Creating checkout object");
    let mut checkout = Checkout::default();
    debug!("Initializing checkout");
    match checkout.init() {
        Ok(()) => {
            trace!("Checkout creation successful");
        },
        Err(e) => {
            error!("Checkout creation failed: {}", e);
            return Err(e);
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
            error!("Stage creation failed: {}", e);
            return Err(e);
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
            error!("Logs creation failed: {}", e);
            return Err(e);
        }
    }
    
    info!("Walking current directory");
    match stage_dir_all(&checkout, &mut logs, &mut stage, PathBuf::from("."), vec![".h2", ".git", "target", "perf.data", "src"]) {
        Ok(()) => {
            debug!("Walk successful");
        },
        Err(e) => {
            error!("Walk failed: {}", e);
            return Err(e);
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
            trace!("Entry id: {:?}", &id);
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

fn diff_dir_all<T: Into<PathBuf>, V: IntoIterator>(checkout: &Checkout, logs: &Logs, path: T, ignore: V)
                                                   -> Result<(), io::Error> where V::Item: Into<PathBuf> {
    let mut to_visit = vec![checkout.path.join(path.into())];
    let to_ignore: HashSet<PathBuf> = HashSet::from_iter(ignore.into_iter().map(|x| {x.into()}));

    info!("Diffing directory tree");
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
            trace!("Entry id: {:?}", &id);
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

            debug!("Creating file index");
            match logs.diff_path(&info) {
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
