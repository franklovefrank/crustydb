use crate::heapfile::HeapFile;
use crate::page::PageIter;
use crate::page::Page;
use common::ids::{ContainerId, PageId, TransactionId};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU16, Ordering};


#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    hf: Arc<HeapFile>,
    pid: PageId,
    index: usize
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the container_id, tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(container_id: ContainerId, tid: TransactionId, hf: Arc<HeapFile>) -> Self {

        /* Create a new heapfile iterator */
        let heapfileiterator = HeapFileIterator {
            hf: hf,
            pid: 0,
            index: 0
        };
        return heapfileiterator;
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {

        /* Clone the hf field */
        let heapfile = self.hf.clone();

        /* Check whether we need to return None */
        if self.pid >= heapfile.num_pages() as PageId
        {
            return None;
        }

        /* Get the page from the heapfile given the pid argument */
        let page = heapfile.read_page_from_file(self.pid);

        /* Make sure the page is valid */
        let valid_page = match page {
            Ok(page) => page,
            _ => panic!("Page not valid"), /* TODO : do we return None? Or panic!? */
        };

        /* Create an iterator for this page */
        let page_iter = valid_page.into_iter();

        let mut temp = 0;

        /* Loop through every valid value and return the (index)th valid value of that page */
        for val in page_iter
        {
            if self.index == temp
            {
                self.index +=1;
                return Some(val)
            }
            temp += 1;
        }

        /* If we reach this point, we have already finished processing the page */
        /* Try to return the first valid value of the next page */

        self.index = 0; /* Reset the index for the next page */
        self.pid += 1; /* Increment pid to tell the iterator to get the next page */

        /* Recursively call next on the next pid */
        return self.next();
    }
}