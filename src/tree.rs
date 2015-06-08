use std::borrow::Borrow;
use std::marker::PhantomData;

use std::io;
use std::mem;
use std::slice;
use std::fmt;

pub trait BufItem: Copy + Ord + fmt::Debug {}

// anything that implements copy can simply be addressed directly as a buffer
impl<T: Copy + Ord + fmt::Debug> BufItem for T {}

#[derive(Debug)]
pub struct BufTree<T: io::Read + io::Write + io::Seek + fmt::Debug, V: BufItem> {
    head: BufTreeHead,
    buffer: T,
    phantom: PhantomData<V>
}

#[derive(Debug, Clone, Copy)]
struct BufNodeHead {
    // index of this node
    idx: u64,
    // number of data items
    len: usize,
    // whether this node is a leaf or no
    leaf: u8
}

#[derive(Debug)]
struct BufNode<T: BufItem> {
    head: BufNodeHead,
    items: Vec<T>,
    next: Vec<u64>
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
        let mut head: BufTreeHead = mem::uninitialized();
        let head_buf = slice::from_raw_parts_mut(&mut head as *mut _ as *mut _,
                                                 mem::size_of::<BufTreeHead>());
        // read into it
        try!(buffer.read(head_buf));
        // forget our buffer
        mem::forget(head_buf);
        // return it
        Ok(head)
    }

    fn write_node(&mut self, node: &BufNode<V>) -> io::Result<()> {
        // write a node
        try!(self.buffer.seek(io::SeekFrom::Start(node.head.idx)));
        // create the slice we care about
        let head_buf = unsafe {
            slice::from_raw_parts(&node.head as *const _ as *const _,
                                  mem::size_of::<BufNodeHead>())
        };
        // write that
        try!(self.buffer.write_all(head_buf));
        // create a buffer for the items
        let items_buf = unsafe {slice::from_raw_parts(node.items.as_ptr() as *const _,
                                                      node.items.len() * mem::size_of::<V>())};
        // write that
        try!(self.buffer.write_all(items_buf.as_ref()));
        mem::forget(items_buf);
        
        if node.next.len() > 0 {
            // create the right kind of slice
            let next_buf = unsafe {slice::from_raw_parts(node.next.as_ptr() as *const _,
                                                         node.next.len() * ::std::u64::BYTES)};
            // write that
            try!(self.buffer.write_all(next_buf));
            mem::forget(next_buf);
            Ok(())
        } else {
            Ok(())
        }
    }

    pub unsafe fn items_at_idx(&mut self, idx: u64) -> io::Result<Vec<V>> {
        // sometimes we just want the items at an index
        match self.read_node(idx) {
            Err(e) => Err(e),
            Ok(node) => Ok(node.items)
        }
    }

    unsafe fn read_node(&mut self, idx: u64) -> io::Result<BufNode<V>> {
        // unsafe because the data could be garbage
        // seek to the given position
        try!(self.buffer.seek(io::SeekFrom::Start(idx)));
        // read the node
        // create a header object
        let mut head: BufNodeHead = mem::uninitialized();
        // create a slice that points to it
        let head_buf = slice::from_raw_parts_mut(&mut head as *mut _ as *mut _,
                                                 mem::size_of::<BufNodeHead>());
        // read into that slice
        try!(self.buffer.read(head_buf));
        // forget that slice
        mem::forget(head_buf);
        
        // check head idx
        if head.idx != idx {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      format!("Node header idx ({}) did not match given idx ({})",
                                              head.idx, idx)));
        }

        // check head len
        if head.len > self.head.size {
            return Err(io::Error::new(io::ErrorKind::InvalidData,
                                      format!("Node header was of greater length ({}) than tree size ({})",
                                              head.len, self.head.size)));
        }

        let vec_len = {
            if head.leaf == 0 {
                // no further reads
                head.len
            } else {
                // will read next list, read full node size to
                // position cursor correctly
                self.head.size
            }
        } * mem::size_of::<V>();
        let mut items_buf = Vec::with_capacity(vec_len);
        items_buf.set_len(vec_len);
        try!(self.buffer.read(items_buf.as_mut()));
        let items = Vec::from_raw_parts(items_buf.as_mut_ptr() as *mut _,
                                        head.len,
                                        items_buf.capacity() / mem::size_of::<V>());
        mem::forget(items_buf);
        let mut next;
        if head.leaf == 0 {
            // create our buffer
            next = Vec::with_capacity((head.len + 1));
            next.set_len(head.len + 1);
            // create a slice that points to it
            let next_buf = slice::from_raw_parts_mut(next.as_ptr() as *mut _,
                                                     (head.len + 1) * ::std::u64::BYTES);
            // read into the slice
            try!(self.buffer.read(next_buf));
            // forget the slice
            mem::forget(next_buf);
        } else {
            next = vec![];
        }
        Ok(BufNode {
            head: head,
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

    fn delete_node(&mut self, idx: u64) -> io::Result<()> {
        if idx == self.head.last - (mem::size_of::<BufNodeHead>() as u64
                                    + mem::size_of::<V>() as u64 *
                                    (self.head.size * 2 + 1) as u64) {
            // instead of writing a gone, just decrement last
            self.head.last = idx;
        } else {
            // seek to the given index
            try!(self.buffer.seek(io::SeekFrom::Start(idx)));
            // create the gone item
            let gone = BufGone {
                idx: idx,
                next: self.head.gone
            };
            // create the slice we care about
            let buffer = unsafe {slice::from_raw_parts(&gone as *const _ as *const _,
                                                       mem::size_of::<BufGone>())};
            // write that to the buffer
            try!(self.buffer.write_all(buffer));
            // update tree metadata
            self.head.gone = Some(idx);
        }
        // write the metadata
        self.write_meta()
    }

    fn new_idx(&mut self) -> io::Result<u64> {
        // return the next empty index for a node, incrementing the internal
        // counters as necessary
        match self.head.gone {
            None => {
                let idx = self.head.last;
                self.head.last += mem::size_of::<BufNodeHead>() as u64 +
                    mem::size_of::<V>() as u64 * (self.head.size) as u64 +
                    ::std::u64::BYTES as u64 * (self.head.size + 1) as u64;
                Ok(idx)
            },
            Some(idx) => {
                let gone = try!(unsafe {self.read_gone(idx)});
                self.head.gone = gone.next;
                Ok(idx)
            }
        }
    }

    pub fn contains<K: Borrow<V>>(&mut self, as_item: K) -> io::Result<bool> {
        match self.get(as_item) {
            Err(e) => Err(e),
            Ok(None) => Ok(false),
            Ok(Some(_)) => Ok(true)
        }
    }

    pub fn get<K: Borrow<V>>(&mut self, as_item: K) -> io::Result<Option<V>> {
        // check for a root node
        let root_idx = match self.head.root {
            None => {
                return Ok(None);
            },
            Some(idx) => idx
        };

        // read the root node
        trace!("reading node");
        let mut current = try!(unsafe {self.read_node(root_idx)});
        trace!("read node: {:?}", &current);
        // ensure there's at least one item in the root node
        if current.items.is_empty() {
            return Ok(None);
        }

        let item = as_item.borrow();

        trace!("Searching with item: {:?}", item);

        // loop until we get to a leaf
        loop {
            let next_index = match current.items.binary_search(item) {
                Ok(idx) => {
                    // item found
                    return Ok(Some(current.items.remove(idx)));
                },
                Err(idx) => {
                    if current.head.leaf != 0 {
                        // item not in tree
                        return Ok(None);
                    } else {
                        // keep searching
                        idx
                    }
                }
            };

            current = try!(unsafe {self.read_node(current.next[next_index])});
        }
    }

    pub fn remove<K: Borrow<V>>(&mut self, as_item: K) -> io::Result<Option<V>> {
        // check for a root node
        let root_idx = match self.head.root {
            None => {
                return Ok(None);
            },
            Some(idx) => idx
        };

        // read the root node
        let mut current = try!(unsafe {self.read_node(root_idx)});
        // ensure there's at least one item in the root node
        if current.items.is_empty() {
            return Ok(None);
        }

        let item = as_item.borrow();
        let mut item_node = None;
        let mut item_current = false;
        let mut item_index = None;
        let mut item_push = false;

        // loop until we find the item or we hit a leaf
        while current.head.leaf == 0 {
            let next_index = {
                if item_node.is_some() {
                    current.head.len
                } else {
                    match current.items.binary_search(item) {
                        Ok(idx) => {
                            item_current = true;
                            item_index = Some(idx);
                            idx
                        },
                        Err(idx) => {
                            idx
                        }
                    }
                }
            };
            let next_idx = current.next[next_index];
            // this means the next after root node is read twice, oh well.
            let mut next = try!(unsafe {self.read_node(next_idx)});

            // ensure that the next node can support a deletion
            if next.head.len >= self.head.size / 2 {
                // it does, nothing to do here
                if item_current {
                    if item_push {
                        item_push = false;
                    } else {
                        item_current = false;
                    }
                    item_node = Some(current);
                }
                current = next;
            } else {
                // favorize left siblings
                let sibling_index = {
                    if next_index > 0 {
                        next_index - 1
                    } else {
                        1
                    }
                };
                let mut sibling = try!(unsafe {self.read_node(current.next[sibling_index])});

                // can the sibling support a deletion?
                if sibling.head.len >= self.head.size / 2 {
                    // pull one from the sibling
                    if sibling_index < next_index {
                        // sibling is to the left
                        let left_item = sibling.items.pop().unwrap();
                        sibling.head.len -= 1;
                        current.items.push(left_item);
                        current.items.swap(sibling_index, current.head.len);
                        let sep_item = current.items.pop().unwrap();
                        next.items.insert(0, sep_item);
                        next.head.len += 1;
                        // move the next value if the sibling isn't a leaf
                        if sibling.head.leaf == 0 {
                            let left_next = sibling.next.pop().unwrap();
                            next.next.insert(0, left_next);
                        }
                    } else {
                        // sibling is to the right
                        let right_item = sibling.items.remove(0);
                        sibling.head.len -= 1;
                        current.items.push(right_item);
                        current.items.swap(next_index, current.head.len);
                        let sep_item = current.items.pop().unwrap();
                        next.items.push(sep_item);
                        if item_current {
                            item_push = true;
                            item_index = Some(next.head.len);
                        }
                        next.head.len += 1;
                        // move the next value if the sibling isn't a leaf
                        if sibling.head.leaf == 0 {
                            let right_next = sibling.next.remove(0);
                            next.next.push(right_next);
                        }
                    }

                    // save everything
                    try!(self.write_node(&sibling));
                    try!(self.write_node(&next));
                    try!(self.write_node(&current));

                    // update current
                    if item_current {
                        if item_push {
                            item_push = false;
                        } else {
                            item_current = false;
                        }
                        item_node = Some(current);
                    }
                    current = next;
                } else {
                    // merge the two nodes
                    if sibling_index < next_index {
                        // sibling is to the left
                        let sep_item = current.items.remove(sibling_index);
                        current.next.remove(next_index);
                        current.head.len -= 1;
                        sibling.items.push(sep_item);
                        sibling.items.extend(next.items);
                        sibling.next.extend(next.next);
                        sibling.head.len = sibling.items.len();

                        // write everything
                        // check to see if we've emptied the root node
                        if current.head.len == 0 {
                            // this only happens if current is the root node
                            self.head.root = Some(sibling.head.idx);
                            try!(self.write_meta());
                            try!(self.delete_node(current.head.idx));
                        } else {
                            try!(self.write_node(&current));
                        }

                        // write the rest of the nodes
                        try!(self.delete_node(next.head.idx));
                        try!(self.write_node(&sibling));
                        if item_current {
                            if item_push {
                                item_push = false;
                            } else {
                                item_current = false;
                            }
                            item_node = Some(current);
                        }
                        current = sibling;
                    } else {
                        // sibling is to the right
                        let sep_item = current.items.remove(next_index);
                        current.next.remove(sibling_index);
                        current.head.len -= 1;
                        next.items.push(sep_item);
                        if item_current {
                            item_push = true;
                            item_index = Some(next.head.len);
                        }
                        next.items.extend(sibling.items);
                        next.next.extend(sibling.next);
                        next.head.len = next.items.len();

                        // write everything
                        // check to see if we've emptied the root node
                        if current.head.len == 0 {
                            // this only happens if current is the root node
                            self.head.root = Some(next.head.idx);
                            try!(self.write_meta());
                            try!(self.delete_node(current.head.idx));
                        } else {
                            try!(self.write_node(&current));
                        }

                        try!(self.delete_node(sibling.head.idx));
                        try!(self.write_node(&next));
                        if item_current {
                            if item_push {
                                item_push = false;
                            } else {
                                item_current = false;
                            }
                            item_node = Some(current);
                        }
                        current = next;
                    }
                }
            }
        }

        // at this point, current is a leaf node, supporting at least one deletion
        match item_node {
            None => {
                // look for the item in the leaf
                match current.items.binary_search(item) {
                    Ok(idx) => {
                        let node_item = current.items.remove(idx);
                        current.head.len -= 1;
                        try!(self.write_node(&current));
                        Ok(Some(node_item))
                    },
                    Err(_) => {
                        // item not in tree
                        Ok(None)
                    }
                }
            },
            Some(mut node) => {
                // swap the relevant item out
                let sep = current.items.pop().unwrap();
                current.head.len -= 1;
                node.items.push(sep);
                node.items.swap(item_index.unwrap(), node.head.len);
                let node_item = node.items.pop().unwrap();
                try!(self.write_node(&node));
                try!(self.write_node(&current));
                Ok(Some(node_item))
            }
        }
    }

    pub fn insert<K: Into<V>>(&mut self, to_item: K) -> io::Result<Option<V>> {
        match unsafe {self.insert_idx(to_item)} {
            Err(e) => Err(e),
            Ok(Ok(_)) => Ok(None),
            Ok(Err(item)) => Ok(Some(item))
        }
    }

    pub unsafe fn insert_idx<K: Into<V>>(&mut self, to_item: K) -> io::Result<Result<u64, V>> {
        // there are certain cases where we care to know where the item was written
        let mut item = to_item.into();

        // check for a root node
        let root_idx = match self.head.root {
            None => {
                // Create the root node
                let node = BufNode {
                    head: BufNodeHead {
                        idx: try!(self.new_idx()),
                        len: 1,
                        leaf: 1
                    },
                    items: vec![item],
                    next: vec![]
                };
                try!(self.write_node(&node));
                // set the root node
                self.head.root = Some(node.head.idx);
                // save the metadata
                try!(self.write_meta());
                return Ok(Ok(node.head.idx));
            },
            Some(idx) => idx
        };

        // read the root node
        let mut current = try!(self.read_node(root_idx));
        let mut sep;

        // check if the root node is full
        if current.head.len == self.head.size {
            // split the node
            // pick a median value
            let index = current.head.len / 2;
            // create a new right node
            let right_node = BufNode {
                head: BufNodeHead {
                    idx: try!(self.new_idx()),
                    len: current.head.len - index - 1,
                    leaf: current.head.leaf
                },
                items: current.items.split_off(index + 1),
                next: {
                    if current.head.leaf != 0 {
                        vec![]
                    } else {
                        current.next.split_off(index + 1)
                    }
                }
            };

            // update our separator value
            sep = current.items.pop().unwrap();
            let finished = item == sep;
            let to_return;
            // update current's len
            current.head.len = current.items.len();

            // create a new root node
            let root_node = BufNode {
                head: BufNodeHead {
                    idx: try!(self.new_idx()),
                    len: 1,
                    leaf: 0
                },
                items: vec![{
                    if finished {
                        to_return = sep;
                        item
                    } else {
                        to_return = item;
                        sep
                    }
                }],
                next: vec![current.head.idx, right_node.head.idx]
            };

            // write everything
            try!(self.write_node(&current));
            try!(self.write_node(&right_node));
            try!(self.write_node(&root_node));

            // update the meta info
            self.head.root = Some(root_node.head.idx);
            try!(self.write_meta());

            // update current
            if finished {
                return Ok(Err(to_return));
            }
            // "clear" item, even though this does nothing
            item = to_return;
            // also reset current
            current = root_node;
        }

        while current.head.leaf == 0 {
            // figure out which next node we need to get
            let next_index = match current.items.binary_search(&item) {
                Ok(idx) => {
                    current.items.push(item);
                    current.items.swap(idx, current.head.len);
                    let node_item = current.items.pop().unwrap();
                    try!(self.write_node(&current));
                    return Ok(Err(node_item));
                },
                Err(idx) => idx
            };
            let next = *current.next.get(next_index).unwrap();

            // read the node
            let mut next_node = try!(self.read_node(next));

            // see if we need to split the node
            if next_node.head.len < self.head.size {
                // just update the next node
                current = next_node;
            } else {
                // create a new right node
                // pick a median value
                let index = next_node.head.len / 2;

                // create a new right node
                let right_node = BufNode {
                    head: BufNodeHead {
                        idx: try!(self.new_idx()),
                        len: next_node.head.len - index - 1,
                        leaf: next_node.head.leaf
                    },
                    items: next_node.items.split_off(index + 1),
                    next: {
                        if next_node.head.leaf != 0 {
                            vec![]
                        } else {
                            next_node.next.split_off(index + 1)
                        }
                    }
                };

                // pop off the separator
                sep = next_node.items.pop().unwrap();
                let routing = {
                    if item < sep {
                        0
                    } else if item > sep {
                        1
                    } else {
                        2
                    }
                };
                let to_return;
                // update the len
                next_node.head.len = next_node.items.len();

                current.next.insert(next_index + 1, right_node.head.idx);
                current.items.insert(next_index, {
                    if routing == 2 {
                        to_return = sep;
                        item
                    } else {
                        to_return = item;
                        sep
                    }
                });
                current.head.len += 1;

                // write everything
                try!(self.write_node(&right_node));
                try!(self.write_node(&next_node));
                try!(self.write_node(&current));

                // update current
                if routing == 0 {
                    current = next_node;
                } else if routing == 1 {
                    current = right_node;
                } else {
                    return Ok(Err(to_return));
                }

                // "clear" item
                item = to_return;
            }
        }

        // at this point current is a leaf node with space to insert our item
        match current.items.binary_search(&item) {
            Ok(idx) => {
                // item was found in the list, swap them
                current.items.push(item);
                current.items.swap(idx, current.head.len);
                let node_item = current.items.pop().unwrap();
                try!(self.write_node(&current));
                Ok(Err(node_item))
            },
            Err(idx) => {
                // insert the item, preserving order
                current.items.insert(idx, item);
                current.head.len += 1;
                try!(self.write_node(&current));
                Ok(Ok(current.head.idx))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[test]
    fn test_tree_basic() {
        let mut tree: BufTree<_, u64> = BufTree::default();
        assert_eq!(tree.contains(35).unwrap(), false);
        assert_eq!(tree.insert(35).unwrap(), None);
        assert_eq!(tree.insert(35).unwrap(), Some(35));
        assert_eq!(tree.contains(35).unwrap(), true);
        assert_eq!(tree.get(35).unwrap(), Some(35));
        assert_eq!(tree.remove(35).unwrap(), Some(35));
        assert_eq!(tree.remove(35).unwrap(), None);
        assert_eq!(tree.contains(35).unwrap(), false);
    }

    #[test]
    fn test_tree_long() {
        let mut tree: BufTree<_, u64> = BufTree::default();
        const NUMBER_TO_TEST:u64 = 100;
        for i in 0..NUMBER_TO_TEST {
            assert_eq!(tree.insert(i).unwrap(), None);
        }
        for i in 0..NUMBER_TO_TEST {
            assert_eq!(tree.contains(i).unwrap(), true);
        }
        for i in (0..NUMBER_TO_TEST).rev() {
            assert_eq!(tree.remove(i).unwrap(), Some(i));
            assert_eq!(tree.remove(i).unwrap(), None);
        }
        for i in 0..NUMBER_TO_TEST {
            assert_eq!(tree.contains(i).unwrap(), false);
        }
    }

    fn bench_contains(b: &mut Bencher, number: u64) {
        // create the tree
        let mut tree: BufTree<_, u64> = BufTree::default();
        for i in 0..number {
            assert_eq!(tree.insert(i).unwrap(), None);
        }
        b.iter(|| tree.contains(number / 2));
    }

    #[bench]
    fn bench_contains_10000(b: &mut Bencher) {
        bench_contains(b, 10000)
    }

    #[bench]
    fn bench_contains_1000(b: &mut Bencher) {
        bench_contains(b, 1000)
    }

    #[bench]
    fn bench_contains_100(b: &mut Bencher) {
        bench_contains(b, 100)
    }
}
