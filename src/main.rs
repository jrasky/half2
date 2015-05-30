#![feature(dir_entry_ext)]
#![feature(symlink_metadata)]
#![feature(path_relative_from)]
#![feature(file_type)]
#[macro_use]
extern crate log;
extern crate env_logger;

use std::path::PathBuf;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::borrow::Borrow;

use std::fs;
use std::io;

#[derive(Debug)]
struct Stage {
    path: PathBuf
}

#[derive(Debug)]
struct Checkout {
    path: PathBuf,
    ignore: HashSet<PathBuf>
}

#[derive(Debug)]
struct PathInfo {
    path: PathBuf,
    pub id: PathBuf
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
                return Err(e);
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
        Checkout::new::<_, PathBuf, _>(".", vec![])
    }
}

impl Checkout {
    pub fn new<T: Into<PathBuf>, U: Into<PathBuf>, V: IntoIterator<Item=U>>(path: T, ignore: V) -> Checkout {
        Checkout {
            path: path.into(),
            ignore: HashSet::from_iter(ignore.into_iter().map(|x| {x.into()}))
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

    pub fn ignore<T: Into<PathBuf>>(&mut self, path: T) -> bool {
        self.ignore.insert(path.into())
    }

    pub fn track<T: Borrow<PathBuf>>(&mut self, path: T) -> bool {
        self.ignore.remove(path.borrow())
    }

    pub fn is_ignored<T: Borrow<PathBuf>>(&self, path: T) -> bool {
        self.ignore.contains(path.borrow())
    }

    pub fn stage_dir_all<T: Into<PathBuf>>(&self, stage: &mut Stage, path: T) -> Result<(), io::Error> {
        let mut to_visit = vec![self.path.join(path.into())];

        info!("Copying directory tree");
        while !to_visit.is_empty() {
            trace!("Popping directory from queue");
            let dir = to_visit.pop().unwrap();
            debug!("Reading directory {:?}", dir);
            for item in match fs::read_dir(dir) {
                Ok(iter) => iter,
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
                if self.is_ignored(&id) {
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
                
                // create the PathInfo object
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
    trace!("Ignoring repo directory");
    checkout.ignore(".h2");
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
    
    info!("Walking current directory");
    match checkout.stage_dir_all(&mut stage, PathBuf::from(".")) {
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

    debug!("Creating ./.h2/logs");
    match fs::create_dir("./.h2/logs") {
        Err(e) => {
            error!("Failed to create directory \".h2/logs\": {}", e);
            return Err(e);
        },
        Ok(_) => {
            trace!("Directory created");
        }
    }

    Ok(())
}

