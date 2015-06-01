#![feature(core)]
#![feature(collections)]
#![feature(dir_entry_ext)]
#![feature(symlink_metadata)]
#![feature(path_relative_from)]
#![feature(file_type)]
#![feature(associated_consts)]
#[macro_use]
extern crate log;
extern crate env_logger;

// general TODO:
// - create our own error type and use that everywhere
// - unify error handling to be more descriptive (replace try!, unwrap)

use std::path::PathBuf;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::marker::PhantomData;

use std::fs;
use std::io;
use std::fmt;
use std::mem;
use std::slice;
use std::ops;

trait BufItem: Ord + fmt::Debug {
    fn as_buf(&self) -> &[u8];
    fn from_buf(&[u8]) -> Self;

    // For some reason we can't use associated constants if all we have is a
    // trait, so instead we must use static functions. Oh joy.
    fn buf_len() -> usize;
}

#[derive(Debug)]
struct Stage {
    path: PathBuf
}

#[derive(Debug)]
struct Checkout {
    path: PathBuf
}

#[derive(Debug)]
struct PathInfo {
    path: PathBuf,
    pub id: PathBuf
}

#[derive(Debug)]
struct Logs {
    path: PathBuf
}

#[derive(Debug, Clone, Copy)]
struct BufNodeHead {
    // index of this node
    idx: u64,
    // number of data items
    len: usize,
    // whether this node is a leaf or no
    leaf: bool
}

#[derive(Debug)]
struct BufNode<T: BufItem> {
    head: BufNodeHead,
    items: Vec<T>,
    next: Option<Vec<u64>>
}

#[derive(Debug, Clone, Copy)]
struct BufGone {
    // index of this node
    idx: u64,
    // index of the next deleted node
    next: Option<u64>
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BufTreeHead {
    // number of elements in each node
    size: usize,
    // index of the first byte past all nodes
    last: u64,
    // index of the root node
    root: Option<u64>,
    // index of the last deleted node
    gone: Option<u64>
}

#[derive(Debug)]
struct BufTree<T: io::Read + io::Write + io::Seek + fmt::Debug, V: BufItem> {
    head: BufTreeHead,
    buffer: T,
    phantom: PhantomData<V>
}

impl<V: BufItem> Default for BufTree<io::Cursor<Vec<u8>>, V> {
    fn default() -> BufTree<io::Cursor<Vec<u8>>, V> {
        match BufTree::new(io::Cursor::new(vec![]), 6) {
            Ok(tree) => tree,
            Err(e) => {
                // since we're initializing in memory this should never happen
                panic!("Failed to initialize BufTree in memory: {}", e);
            }
        }
    }
}

impl<T: io::Read + io::Write + io::Seek + fmt::Debug, V: BufItem> BufTree<T, V> {
    // TODO: insert size checks for all reads
    // TODO: check item indexes to ensure they aren't written to the wrong place
    // TODO: make the inserts more efficient
    // TODO: maybe implement the one-pass splitting method?

    pub fn new(buffer: T, size: usize) -> io::Result<BufTree<T, V>> {
        let mut tree = BufTree {
            head: BufTreeHead {
                size: size,
                last: mem::size_of::<BufTreeHead>() as u64,
                root: None,
                gone: None
            },
            buffer: buffer,
            phantom: PhantomData
        };
        // write meta info since it's a new tree
        try!(tree.write_meta());
        // return said tree
        Ok(tree)
    }

    pub unsafe fn from_buffer(mut buffer: T) -> io::Result<BufTree<T, V>> {
        // unsafe because there's no guarentee that this buffer is correctly formed
        Ok(BufTree {
            head: try!(Self::read_meta(&mut buffer)),
            buffer: buffer,
            phantom: PhantomData
        })
    }

    fn write_meta(&mut self) -> io::Result<()> {
        // seek to the start of the file
        try!(self.buffer.seek(io::SeekFrom::Start(0)));
        // create the slice we care about
        let buffer = unsafe {slice::from_raw_parts(&self.head as *const _ as *const _,
                                                   mem::size_of::<BufTreeHead>())};
        // write that to the buffer
        self.buffer.write_all(buffer)
    }

    unsafe fn read_meta(buffer: &mut T) -> io::Result<BufTreeHead> {
        // unsafe because data could be garbage
        // seek to the start of the file
        try!(buffer.seek(io::SeekFrom::Start(0)));
        // create our buffer
        let mut head_buf = Vec::with_capacity(mem::size_of::<BufTreeHead>());
        // read into it
        try!(buffer.read(head_buf.as_mut()));
        // transmute to our desired type
        let head_ptr = head_buf.as_ptr() as *const BufTreeHead;
        // return it
        Ok(*head_ptr.as_ref().unwrap())
    }

    fn write_node(&mut self, node: &BufNode<V>) -> io::Result<()> {
        // write a node
        try!(self.buffer.seek(io::SeekFrom::Start(node.head.idx)));
        // create the slice we care about
        let head_buf = unsafe {
            slice::from_raw_parts(&node.head as *const _ as *const _, mem::size_of::<BufNodeHead>())
        };
        // write that
        try!(self.buffer.write_all(head_buf));
        // create a buffer for the items
        let mut items_buf = Vec::with_capacity({node.head.len} * V::buf_len());
        for item in node.items.iter() {
            items_buf.push_all(item.as_buf());
        }
        // write that
        try!(self.buffer.write_all(items_buf.as_ref()));
        match node.next {
            None => Ok(()),
            Some(ref next) => {
                // create the right kind of slice
                let next_buf = unsafe {slice::from_raw_parts(next.as_ptr() as *const _,
                                                             next.len() * std::u64::BYTES)};
                // write that, use as return value
                self.buffer.write_all(next_buf)
            }
        }
    }

    unsafe fn read_node(&mut self, idx: u64) -> io::Result<BufNode<V>> {
        // unsafe because the data could be garbage
        // seek to the given position
        try!(self.buffer.seek(io::SeekFrom::Start(idx)));
        // read the node
        let mut head_buf = vec![0; mem::size_of::<BufNodeHead>()];
        try!(self.buffer.read(head_buf.as_mut()));
        let head = {
            let ptr = head_buf.as_ptr() as *const BufNodeHead;
            ptr.as_ref().unwrap()
        };
        let mut items_buf = vec![0; {
            if head.leaf {
                // no further reads
                head.len
            } else {
                // will read next list, read full node size to
                // position cursor correctly
                self.head.size
            }
        } * V::buf_len()];
        try!(self.buffer.read(items_buf.as_mut()));
        let mut items = Vec::with_capacity(head.len);
        let mut item_ptr = items_buf.as_ptr();
        for _ in 0..head.len {
            items.push(V::from_buf(slice::from_raw_parts(item_ptr, V::buf_len())));
            item_ptr = item_ptr.offset(V::buf_len() as isize);
        }
        let mut next = None;
        if !head.leaf {
            let mut next_buf = vec![0; (head.len + 1) * std::u64::BYTES];
            try!(self.buffer.read(next_buf.as_mut()));
            next = Some(Vec::from_raw_buf(next_buf.as_ptr() as *const u64, head.len + 1));
        }
        Ok(BufNode {
            head: *head,
            items: items,
            next: next
        })
    }

    unsafe fn read_gone(&mut self, idx: u64) -> io::Result<BufGone> {
        // unsafe because the data could be garbage
        // seek to the given position
        try!(self.buffer.seek(io::SeekFrom::Start(idx)));
        // create a buffer
        let mut gone_buf = Vec::with_capacity(mem::size_of::<BufGone>());
        // read into it
        try!(self.buffer.read(gone_buf.as_mut()));
        // transmute into our desired type
        let gone_ptr = gone_buf.as_ptr() as *const BufGone;
        // return it
        Ok(*gone_ptr.as_ref().unwrap())
    }

    fn new_idx(&mut self) -> io::Result<u64> {
        // return the next empty index for a node, incrementing the internal
        // counters as necessary
        match self.head.gone {
            None => {
                let idx = self.head.last;
                self.head.last += mem::size_of::<BufNodeHead>() as u64 +
                    V::buf_len() as u64 * (self.head.size * 2 + 1) as u64;
                Ok(idx)
            },
            Some(idx) => {
                let gone = try!(unsafe {self.read_gone(idx)});
                self.head.gone = gone.next;
                Ok(idx)
            }
        }
    }

    pub fn insert<K: Into<V>>(&mut self, to_item: K) -> io::Result<bool> {
        let item = to_item.into();

        // check for a root node
        let root_idx = match self.head.root {
            None => {
                // Create the root node
                let node = BufNode {
                    head: BufNodeHead {
                        idx: try!(self.new_idx()),
                        len: 1,
                        leaf: false
                    },
                    items: vec![item],
                    next: None
                };
                try!(self.write_node(&node));
                // set the root node
                self.head.root = Some(node.head.idx);
                // save the metadata
                try!(self.write_meta());
                return Ok(false);
            },
            Some(idx) => idx
        };

        // read the root node
        let mut current = try!(unsafe {self.read_node(root_idx)});
        let mut path = vec![];
        while !current.head.leaf {
            if &item > current.items.last().unwrap() {
                let next = *current.next.as_ref().unwrap().last().unwrap();
                path.push(current);
                current = try!(unsafe {self.read_node(next)});
            } else {
                for i in 0..current.head.len {
                    if &item == current.items.get(i).unwrap() {
                        // item already in tree
                        return Ok(true);
                    } else if &item < current.items.get(i).unwrap() {
                        let next = current.next.as_ref().unwrap()[i];
                        path.push(current);
                        current = try!(unsafe {self.read_node(next)});
                        break;
                    }
                }
            }
        }

        // insert the element into the node
        // default to the last element
        let mut index = current.items.len();
        for i in 0..current.head.len {
            if &item == current.items.get(i).unwrap() {
                // item already in tree
                return Ok(true);
            } else if &item < current.items.get(i).unwrap() {
                index = i;
                break;
            }
        }
        current.items.insert(index, item);
        // increment the len
        current.head.len += 1;

        // we should now have a current leaf, and a path of nodes leading to it
        if current.head.len <= self.head.size {
            // no need to propagate anything, just save
            try!(self.write_node(&current));
            Ok(false)
        } else {
            // hard case: split the node
            // split at the median value
            index = current.items.len() / 2 + 1;

            // create the new right node
            let mut right_node = BufNode {
                head: BufNodeHead {
                    idx: try!(self.new_idx()),
                    len: current.head.len - index,
                    leaf: true
                },
                items: current.items.split_off(index + 1),
                next: None
            };

            // pop off our separator value
            let mut sep = current.items.pop().unwrap();
            let mut idx = current.head.idx;
            // reset the len
            current.head.len = current.items.len();
            // write both nodes
            try!(self.write_node(&right_node));
            // at this point current is already the correct left node
            try!(self.write_node(&current));

            // now the even harder part, which is propagating everything up the tree.
            loop {
                // grab the next element up the tree
                current = match path.pop() {
                    None => break, // out of path, create new root element
                    Some(item) => item
                };

                // search for our index in the next list
                let mut child_idx = current.head.len + 2;
                for i in 0..current.head.len + 1 {
                    if current.next.as_ref().unwrap()[i] == idx {
                        child_idx = i;
                        break;
                    }
                }

                // panic if we didn't find it
                if child_idx == current.head.len + 2 {
                    panic!("Didn't find idx in parent");
                }

                // insert the info
                current.items.insert(child_idx, sep);
                current.head.len += 1;
                current.next.as_mut().unwrap().insert(child_idx + 1, right_node.head.idx);

                // now have all info, insert separator into node
                if current.head.len < self.head.size {
                    // easy case
                    // write the node
                    try!(self.write_node(&current));
                    // done!
                    return Ok(false);
                } else {
                    // cry a lot
                    // split the list at the median
                    index = current.items.len() / 2 + 1;

                    // create the new right node
                    right_node = BufNode {
                        head: BufNodeHead {
                            idx: try!(self.new_idx()),
                            len: current.head.len - child_idx,
                            leaf: false
                        },
                        items: current.items.split_off(index + 1),
                        next: Some({
                            let mut next = vec![idx];
                            next.extend(current.next.as_mut().unwrap().split_off(index + 2));
                            next
                        })
                    };

                    // get new sep
                    sep = current.items.pop().unwrap();
                    // pop off our next entry
                    current.next.as_mut().unwrap().pop().unwrap();
                    // change idx
                    idx = current.head.idx;
                    // reset current len
                    current.head.len = current.items.len();
                    // write both nodes
                    try!(self.write_node(&right_node));
                    try!(self.write_node(&current));
                    // and loop, propagating the change upwards again.
                }
            }

            // if we've reached this point, create a new root node with just the separator
            let root_node = BufNode {
                head: BufNodeHead {
                    idx: try!(self.new_idx()),
                    len: 1,
                    leaf: false
                },
                items: vec![sep],
                next: Some(vec![idx, right_node.head.idx])
            };

            // write that node
            try!(self.write_node(&root_node));
            // update the tree's root node
            self.head.root = Some(root_node.head.idx);
            // write the meta info
            try!(self.write_meta());
            // done!
            Ok(false)
        }
    }
}

impl PathInfo {
    pub fn new<T: Into<PathBuf>, V: Into<PathBuf>>(path: T, id: V) -> PathInfo {
        PathInfo {
            path: path.into(),
            id: id.into()
        }
    }

    pub fn metadata(&self) -> Result<fs::Metadata, io::Error> {
        fs::symlink_metadata(&self.path)
    }

    pub fn copy<T: Into<PathBuf>>(&self, to: T) -> Result<(), io::Error> {
        match self.metadata() {
            Err(e) => {
                error!("Failed to get metadata for path {:?}: {}", self.path, e);
                Err(e)
            },
            Ok(data) => {
                trace!("Successfully got metadata");
                if data.is_dir() {
                    trace!("Copying as directory");
                    self.copy_dir(to)
                } else if data.is_file() {
                    trace!("Copying as file");
                    self.copy_file(to)
                } else {
                    error!("{} is neither a file or a directory", self.path.display());
                    unimplemented!()
                }
            }
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
        // "add" means update. If anything already exists, it's overwritten.
        info!("Adding path {:?}", path);
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

    pub fn stage_dir_all<T: Into<PathBuf>, V: IntoIterator>(&self, stage: &mut Stage, path: T, ignore: V)
                                                            -> Result<(), io::Error> where V::Item: Into<PathBuf> {
        let mut to_visit = vec![self.path.join(path.into())];
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
                let id = match entry.path().relative_from(&self.path) {
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

                trace!("Getting file type");
                match entry.file_type() {
                    Ok(ref ty) if ty.is_dir() => {
                        debug!("Adding path to visit queue");
                        to_visit.push(entry.path());
                    },
                    Ok(_) => {
                        trace!("Not adding path to visit queue");
                    },
                    Err(e) => {
                        error!("Could not get file type: {}", e);
                        return Err(e);
                    }
                }
                
                trace!("Creating path info object");
                let info = PathInfo::new(entry.path(), id);

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
            }
        }

        trace!("Init finished");
        Ok(())
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
    match checkout.stage_dir_all(&mut stage, PathBuf::from("."), vec![".h2"]) {
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
