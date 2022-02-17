use crate::{heapfile::HeapFile, page};
use crate::page::PageIter;
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the f.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    c_id: ContainerId,
    hf: Arc<HeapFile>,
    cur_page: PageId,
    iter: PageIter
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap f.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let page = HeapFile::read_page_from_file(&hf.clone(), 0);
        match page{
            Err(e) => panic!("something went wrong"),
            Ok(p) =>  HeapFileIterator{
                c_id: container_id, 
                hf: hf, 
                cur_page: 0,
                iter: p.into_iter()}
            }
    }   
}

/// Trait implementation for heap f iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let hf = self.hf.clone(); 
        let page_count = hf.num_pages();
        while self.cur_page < page_count {
            let ret = self.iter.next();
            match ret {
                Some(data) => {
                    return Some(data);
                }
                None => {
                    self.cur_page += 1; 
                    let heapfile = self.hf.clone();
                    let new_page = heapfile.read_page_from_file(self.cur_page);
                    match new_page {
                        Ok(p) => {
                            self.iter = p.into_iter();
                        },
                        Err(e) => return None
                    }
                }
            }
        }
        return None;
    }

}

