#![feature(core)]
#![feature(step_by)]
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

use std::fmt;
use std::fs;
use std::io;
use std::mem;
use std::env;

use tree::*;

mod tree;

fn main() {
    let mut tree: BufTree<_, usize> = BufTree::default();
    
    for i in 0..200000 {
        tree.insert(i + 1).unwrap();
    }

    for i in (200000..0).step_by(-1) {
        assert!(tree.contains(&i).unwrap());
    }
}
