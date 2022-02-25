use crate::page::Page;
use common::ids::PageId;
use common::{CrustyError, PAGE_SIZE};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};
use std::io::{Seek, SeekFrom};

/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization. 
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
    empty_space: Arc<RwLock<Vec<usize>>>, 
    f_lock: Arc<RwLock<File>>,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path and container Id. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(file_path: PathBuf) -> Result<Self, CrustyError> {
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(String::from("could not open file")));
            }
        };
        let new = HeapFile {
            read_count: AtomicU16::new(0),
            write_count: AtomicU16::new(0),
            f_lock: Arc::new(RwLock::new(file)),
            empty_space: Arc::new(RwLock::new(Vec::new())),
        };
        return Ok(new);
    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        let file = self.f_lock.read();
        let metadata = file.unwrap().metadata().unwrap();
        let len = metadata.len();
        let ret : PageId = (len / PAGE_SIZE as u64).try_into().unwrap();
        return ret;
    }

    pub fn find_pages_to_insert(&self, len: usize) -> Vec<PageId> {
        let mut page_ids = Vec::new();
        let empty_space = self.empty_space.read().unwrap();
        let e_iter = empty_space.iter().enumerate();

        for (i, &empty_len) in e_iter{
            let comp = len + 6;
            if empty_len >= comp {
                page_ids.push(i as PageId);
            }
        }
        return page_ids;
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        let page_num: usize = usize::from(self.num_pages());
        let p = usize::from(pid);
        if p < page_num {
            let mut file = self.f_lock.write().unwrap();
            let mut page_buffer = [0; 4096];
            let index = PAGE_SIZE * pid as usize;
            file.seek(SeekFrom::Start(index.try_into().unwrap()))?;
            file.read(&mut page_buffer[..])?;
            Ok(Page::from_bytes(&page_buffer))
        }
        else {
            return Err(CrustyError::CrustyError("invalid pageID".to_string()));
        }
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        let page_num: usize = self.num_pages().into();
        let mut file = self.f_lock.write().unwrap();

        let p_id = page.get_page_id();
        if p_id as usize >= page_num {
            let seek  = file.seek(SeekFrom::End(0));
            match seek {
                Ok(_) => {
                    match file.write(&page.get_bytes()) {
                        Ok(_) => {
                            let free = page.get_largest_free_contiguous_space();
                            let mut empty_space = self.empty_space.write().unwrap();
                            empty_space.push(free);
                            return Ok(());
                        }
                        Err(e) => return Err(CrustyError::IOError("write error".to_string())) 
                    }
                }
                Err(e) =>  return Err(CrustyError::IOError("seek error".to_string()))
            }
        }

       let seek = file.seek(SeekFrom::Start(
            (PAGE_SIZE * p_id as usize).try_into().unwrap(),
        ));
        match seek {
            Err(e) => return Err(CrustyError::IOError("seek error".to_string())),
            Ok(_) => {
                match file.write(&page.get_bytes()) {
                    Err(e) => {
                        return Err(CrustyError::IOError("write error".to_string()));
                    },
                    Ok(_) => {
                        let mut empty_space = self.empty_space.write().unwrap();
                        empty_space[p_id as usize] = page.get_largest_free_contiguous_space();
                        return Ok(());
                    }
                }
            }
        }
    }


}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf()).unwrap();

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.get_bytes();
        println!("before write");
        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.get_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.get_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.get_bytes());

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}
