use crate::heapfile::HeapFile;
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
    t_id: TransactionId,
    c_id: ContainerId,
    hf: Arc<HeapFile>,
    cur: PageIter,
    page_id: PageId
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap f.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let page = HeapFile::read_page_from_file(&hf.clone(), 0);
        match page{
            Err(e) => panic!("something went wrong"),
            Ok(p) =>  HeapFileIterator{c_id: container_id, 
                t_id: tid,
                hf: hf, 
                cur: p.into_iter(),
                page_id: 0}
        }   

    }
}

/// Trait implementation for heap f iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    
    fn next(&mut self) -> Option<Self::Item> {
        while self.page_id <= self.hf.num_pages() {
            match self.cur.next(){
                None => {
                    let file = &self.hf.clone();
                    self.page_id = &self.page_id + 1;
                    let page = HeapFile::read_page_from_file(file, self.page_id);
                    match page{
                        Err(e) => panic!("something went wrong"),
                        Ok(p) => { self.cur = p.into_iter(); }
                    }
                    
                }
                Some(res) => {
                    Some(res);
                }
            }
        }
        None
    }
}
