use crate::buffer_pool::BufferPool;
use crate::heapfile::HeapFile;
use crate::page::Page;
use crate::page::PageIter;
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::{Arc, RwLock};

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the f.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    c_id: ContainerId,
    t_id: TransactionId,
    hf: Arc<HeapFile>,
    bp_lock: Arc<RwLock<BufferPool>>,
    page_id: PageId,
    current: PageIter,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap f.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>,bp_lock: Arc<RwLock<BufferPool>>) -> Self {
        let mut buffer_pool = bp_lock.write().unwrap();
        let page = buffer_pool.get_page(container_id, 0);
        let p: Page;
        if page.is_none(){
            let ret = HeapFile::read_page_from_file(&hf, 0);
            p = match ret{
                Err(_) => {
                    panic!("read failed");
                },
                Ok(page) => {
                    buffer_pool.insert_page(&page, container_id);
                    page
                }
            }     
        }
        else {
            p = page.unwrap();
        }
        
        let c_iter =  Page::into_iter(p);
        HeapFileIterator {
            c_id: container_id,
            t_id: tid,
            hf: hf.clone(),
            bp_lock: bp_lock.clone(),
            page_id: 0,
            current: c_iter
        }
    }
}

/// Trait implementation for heap f iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer_pool = self.bp_lock.write().unwrap();
        while self.hf.num_pages() > self.page_id {
            let res = buffer_pool.get_page(self.c_id, self.page_id);
            if res.is_none(){
                let read_ret = HeapFile::read_page_from_file(&self.hf, self.page_id);
                match read_ret{
                    Err(_) => {
                        self.page_id += 1;
                    }
                    Ok(p) => {
                        buffer_pool.insert_page(&p, self.c_id);
                        let p_iter = Page::into_iter(p);
                        self.current = p_iter;
                    }

                }
            }
            else {
                let res2 = self.current.next();
                if res2.is_none(){
                    self.page_id += 1;
                    let bp_get_page = buffer_pool.get_page(self.c_id, self.page_id);
                    match  bp_get_page{
                        None => {
                            let hf_read = HeapFile::read_page_from_file(&self.hf, self.page_id);
                            match  hf_read{
                                Err(_) => {
                                    return None;
                                },
                                Ok(p) => {
                                    buffer_pool.insert_page(&p, self.c_id);
                                    let p_iter = Page::into_iter(p);
                                    self.current = p_iter;
                                }
                            }
                        },
                        Some(page) => {
                            let p_iter = Page::into_iter(page);
                            self.current = p_iter;
                         }
                    }
                }
                else {
                    let val = res2.unwrap();
                    return Some(val);
                }
            }
        }
        return None;
    }
}


