use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use crate::buffer_pool::BufferPool;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Write, BufReader, Read};
use std::path::{PathBuf, Path};
use std::sync::{Arc, RwLock};
use std::fs::File;




pub struct Heap {
    pub container_id: ContainerId,
    pub num_pages: PageId,
    pub pages: Vec<Vec<u8>>
}

pub fn serialize_heap(heap: &Heap) -> Vec<u8> {
    let container_id = & mut heap.container_id.to_be_bytes().to_vec();
    let num_pages = & mut heap.num_pages.to_be_bytes().to_vec();
    container_id.append(num_pages);
    for p in &heap.pages{
        container_id.append(& mut p.to_vec());
    }
    return container_id.to_vec(); 
}

pub fn deserialize_heap(heap: Vec<u8>) -> Heap {
    let mut dst = [0,0]; //container_id
    let mut dst1= [0,0]; //num_pages
    dst.clone_from_slice(&heap[..2]);
    dst1.clone_from_slice(&heap[2..4]);
    let container_id = u16::from_be_bytes(dst);
   // let num_pages = (heap.len() - 4) / 4096;
    let num_pages = u16::from_be_bytes(dst1);
    let mut pages: Vec<Vec<u8>> = Vec::new();
    let mut i: usize = 0;
   // println!("num pages is {}", num_pages);
    //println!("heap is len {}", heap.len());
    while i < usize::from(num_pages){
        let mut dst2 = [0; PAGE_SIZE];
        let si = usize::from((i*PAGE_SIZE)+4);
        let ei = usize::from(si + PAGE_SIZE);
        dst2.clone_from_slice(&heap[si..ei]);
        pages.push(dst2.to_vec());
        i+=1;
    };
    Heap { container_id:container_id, num_pages: num_pages, pages: pages }
}   



/// The StorageManager struct
pub struct StorageManager {
    pub storage_path: String,
    is_temp: bool,
    hf_map: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    bp: Arc<RwLock<BufferPool>>
}


/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        /* Get a pointer to the heapfiles vector */
        let mut bp = self.bp.write().unwrap();
        let ret = bp.get_page(container_id, page_id);
        if ret.is_some(){
            return ret;
        } 
        else {
            let heapfiles = self.hf_map.read().unwrap().clone();
            if heapfiles.contains_key(&container_id){
                let heapfile = heapfiles[&container_id].clone();
                let ret_page = HeapFile::read_page_from_file(&heapfile, page_id);
                match ret_page {
                    Ok(page) => {
                        bp.insert_page(&page, container_id);
                        return Some(page)
                    }
                    Err(_) => return None
                }
            } else {
                None
            }    
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        /* Get a pointer to the heapfiles vector */
        let heapfiles = self.hf_map.read().unwrap().clone();
        let hf = heapfiles.get(&container_id);
        if hf.is_none(){
            return Err(CrustyError::CrustyError(String::from("write_page: could not write page to file")));
        }
        let mut bp = self.bp.write().unwrap();
        bp.insert_page(&page, container_id);
        let res = hf.unwrap();
        match res.write_page_to_file(page) { 
            Ok(()) => Ok(()),
            Err(_e) => Err(CrustyError::CrustyError(String::from("write_page: could not write page to file")))
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let heapfiles = self.hf_map.read().unwrap().clone();
        let hf = heapfiles.get(&container_id);
        if hf.is_none(){
            panic!("container file does not exist");
        }
        let ret = hf.unwrap().clone();
        let num_pages = HeapFile::num_pages(&ret);
        return num_pages;
    }


    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        panic!("to do")
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        let exists = Path::new(&storage_path).exists();
        if exists {
            let mut hf_map = HashMap::new();
            let mut path = PathBuf::from(&storage_path);
            let paths = fs::read_dir(&path);
            match paths {
                Ok(ps) => { 
                    for path in ps {
                        //println!(" path is {:?}", path.unwrap().path().file_name());
                        let f = File::open(path.unwrap().path());
                        match f {
                            Ok(f) => {
                                let mut buf = BufReader::new(f);
                                let mut buffer: Vec<u8> = Vec::new();
                                match buf.read_to_end(& mut buffer) {
                                    Ok(x) => (),
                                    Err(e) => panic!("could not read into buffer")  
                                }
                                let heap = deserialize_heap(buffer);
                               // println!("container_id: {}, num_pages: {}, len pages {}", heap.container_id, heap.num_pages, heap.pages.len());
                                let mut hf_path = storage_path.clone();
                                hf_path.push_str(&heap.container_id.to_string());
                                let new_hf = HeapFile::new(PathBuf::from(hf_path.clone())).unwrap();
                                for p in heap.pages{
                                    let page = Page::from_bytes(&p);
                                    new_hf.write_page_to_file(page);
                                }
                                hf_map.insert(heap.container_id, Arc::new(new_hf));
                               // println!("hf map len {}", hf_map.len());
                            }
                            Err(e) => panic!("can't open file")
                        }

                    }
                },
                Err(e) => panic!("couldn't find dir")
            }

            return StorageManager{hf_map: Arc::new(RwLock::new(hf_map)), storage_path: storage_path, is_temp: false,bp: Arc::new(RwLock::new(BufferPool::new()))}
        }
        let new_sm = StorageManager{hf_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: false,bp: Arc::new(RwLock::new(BufferPool::new()))};
        new_sm
    }
    
    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string(); 
        let new_sm = StorageManager{hf_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: true,bp: Arc::new(RwLock::new(BufferPool::new()))};
        new_sm
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        _tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        let mut bp = self.bp.write().unwrap();
        let mut v_id = ValueId::new(container_id);
        let heapfiles = self.hf_map.write().unwrap().clone();
        let hf_ret = heapfiles.get(&container_id);
        match hf_ret {
            Some(hf) => {
                let page_ids = hf.find_pages_to_insert(value.len());
                let page_iter = page_ids.iter();
                for &page_id in page_iter {
                    let mut page = match bp.get_page(container_id, page_id) {
                        Some(mut p) => p,
                        None => match hf.read_page_from_file(page_id) {
                            Ok(p) => {
                                bp.insert_page(&p, container_id);
                                p
                            }
                            Err(_) => {
                                panic!("fails to read page from file");
                            }
                        },
                    };
                    let add_value = page.add_value(&value);
                    if add_value.is_some() {
                        let slot_id = add_value.unwrap();
                        bp.write_page(&page, container_id);
                        hf.write_page_to_file(page);
                        v_id.page_id = Some(page_id as PageId);
                        v_id.slot_id = Some(slot_id);
                        //println!("returning v with page_id {}, slot_id {}",v_id.page_id.unwrap(), v_id.slot_id.unwrap());
                        return v_id;

                    }
                }
                let num_pages = hf.num_pages();
                let mut page = Page::new(num_pages);
                let add_value = page.add_value(&value);
                if add_value.is_some() {
                    let slot_id = add_value.unwrap();
                    bp.write_page(&page, container_id);
                    hf.write_page_to_file(page);
                    v_id.page_id = Some(num_pages);
                    v_id.slot_id = Some(slot_id);
                    //println!("returning v with page_id {}, slot_id {}",v_id.page_id.unwrap(), v_id.slot_id.unwrap());
                    return v_id;
                }
                else {
                    panic!("boo");
                }
            },
            None => {
                panic!("invalid containerID {}", container_id);
            }
        }
    }

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut vals = Vec::new();
        let mut i = 0;
        while i < values.len() {
            vals.push(self.insert_value(container_id, values[i].clone(), tid));
            i+=1;
        }
        return vals;
    }


    fn delete_value(&self, id: ValueId, _tid: TransactionId) -> Result<(), CrustyError> {
        match self.get_page(id.container_id, id.page_id.unwrap(), _tid, Permissions::ReadWrite, false) {
            Some(mut page) => match page.delete_value(id.slot_id.unwrap()) {
                Some(_) => {
                    return self.write_page(id.container_id, page, _tid);
                }
                None => {
                    panic!("delete_val: error");
                }
            },
            None => {
                return Ok(());
            }
        }
    }
    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    // fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
    //     if id.page_id.is_none() || id.slot_id.is_none()
    //     {
    //         return Err(CrustyError::CrustyError(String::from("value not found")))
    //     }
    //     let heapfiles = self.hf_map.write().unwrap().clone();
    //     let hf = heapfiles.get(&id.container_id);
    //     if hf.is_none(){
    //         return Err(CrustyError::CrustyError(String::from("delete value err, couldn't find heap file")));
    //     }
    //     let res = hf.unwrap();
    //     match res.read_page_from_file(id.page_id.unwrap())
    //     {
    //         Err(error) => Err(CrustyError::CrustyError(String::from("couldn't read"))),
    //         Ok(mut page) => {
    //             match page.delete_value(id.slot_id.unwrap()){
    //                 None => return Ok(()),
    //                 Some(()) => return Ok(())

    //             }
    //         }
    //     }
    // }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        match self.delete_value(id, _tid) {
            Ok(_) => {
                //println!("deleted ok");
                return Ok(self.insert_value(id.container_id, value, _tid));
            }
            Err(err) => {
               // println!("error deleting");
                return Err(err);
            }
        }
    }

    /// Create a new container to be stored. 
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize 
    /// the container_config, name, container_type, or dependencies
    /// 
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
            let mut hfs = self.hf_map.write().unwrap();
            let path = &mut self.storage_path.clone();
            path.push_str(&container_id.to_string());
           // println!("storage path after create is {:?}", self.storage_path);
            let ret = HeapFile::new(PathBuf::from(path.clone()));
            match ret{
                Err(e) => Err(CrustyError::CrustyError(String::from("container could not be created"))),
                Ok(hf) => {
                    hfs.insert(container_id, Arc::new(hf));
                    return Ok(());
                }
            }
    }
    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut hfs = self.hf_map.write().unwrap();
        let hf = hfs.get(&container_id);
        if hf.is_none(){
            return Err(CrustyError::CrustyError(String::from("container not there")))
        }
        hfs.remove(&container_id);
        let path = &mut self.storage_path.clone();
        path.push_str(&container_id.to_string());
        let ret = fs::remove_file(path);
        match ret{
            Ok(()) => return Ok(()),
            Err(e) => return Err(CrustyError::CrustyError(String::from("couldn't remove file")))
        }
    }



    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        /* Get the heapfile pointer given the provided container_id */
        let hfs = self.hf_map.read().unwrap();
        let hf =  hfs.get(&container_id);
        if hf.is_none(){
            panic!("container id is invalid");
        }
        HeapFileIterator::new(container_id, tid, hf.unwrap().clone())
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        panic!("TODO milestone hs");
    }


    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        let hfs = self.hf_map.read().unwrap().clone();
        for (container_id, hf) in hfs {
            self.remove_container(container_id);
        }
        fs::remove_dir_all(&self.storage_path);
        self.hf_map.write().unwrap().clear();
        self.clear_cache();
        drop(self);
        return Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    fn clear_cache(&self) {
        let mut bp = self.bp.write();
        match bp {
            Ok( mut buffer_pool) => buffer_pool.reset(),
            Err(_) => ()
        }

    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    /// If not a temp SM, this should serialize the mapping between containerID and Heapfile. 
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        if self.is_temp {
            fs::remove_dir_all(&self.storage_path);
        }  
        else {
            let hfs = self.hf_map.read().unwrap().clone();
            let mut dir_path = PathBuf::from(&self.storage_path);
            match fs::create_dir_all(dir_path.clone()) {
                Ok(()) => (),
                Err(e) => panic!("error creating directory")
            }
            for (container_id, hf) in hfs {
                let mut page_vec: Vec<Vec<u8>> = Vec::new(); 
                let num_pages = hf.num_pages();
                for i in 0..num_pages{
                    let cur_page = hf.read_page_from_file(i).unwrap();
                    let page_bytes = cur_page.get_bytes();
                    page_vec.push(page_bytes);
                }
                let temp = Heap{ container_id:container_id, num_pages: num_pages, pages: page_vec};
                let serialized = serialize_heap(&temp);
                //let mut hf_path = dir_path.push(&container_id.to_string());
                let mut path = PathBuf::from(&self.storage_path);
                path.push(&container_id.to_string());
                let mut file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
            {
                Ok(f) => f,
                Err(error) => {
                    panic!("could not open file")
                }
            };
                file.write_all(&serialized);
                self.remove_container(container_id);
            }
        }
        self.clear_cache();
    }


    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.get_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError("Could not read row from CSV".to_string()))
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    /// Shutdown the storage manager. Can call be called by shutdown. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    fn drop(&mut self) {
        self.shutdown();
        drop(self);
    }
}



#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.get_bytes()[..], p2.get_bytes()[..]);
    }

    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x);
        }
    }

    #[test]
    #[ignore]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
