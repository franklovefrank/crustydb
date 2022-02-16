use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::path::{PathBuf, Path};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};


/// The StorageManager struct
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: String,
    is_temp: bool,
    hash_map: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
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
        let map = &*self.hash_map.read().unwrap();
        if map.contains_key(&container_id){
            let hf = map[&container_id].clone();
            let ret = HeapFile::read_page_from_file(&hf, page_id).unwrap();
            return Some(ret);
        } else {
            return None;
        }    
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let map = &*self.hash_map.read().unwrap();
        let mut heap_file = map.get(&container_id);
        HeapFile::write_page_to_file(&heap_file.unwrap(), page);
        Ok(())
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let map = &*self.hash_map.read().unwrap();
        let mut heap_file = map.get(&container_id).unwrap();
        let ret = HeapFile::num_pages(&heap_file);
        ret
    }


    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        let map = &*self.hash_map.read().unwrap();
        if map.contains_key(&container_id){
            let heap_file = map.get(&container_id).unwrap();
            let w_count = heap_file.write_count.load(Ordering::Relaxed);
            let r_count = heap_file.read_count.load(Ordering::Relaxed);
            (r_count, w_count)
        } else {
            (0,0) 
        }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        StorageManager{hash_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: false}
    }

    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        debug!("Making new temp storage_manager {}", storage_path);
        StorageManager{hash_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: false}
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.
    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let map = &*self.hash_map.read().unwrap();
        let mut hf = map.get(&container_id).unwrap();
        let num_pages = HeapFile::num_pages(hf);
        let mut page_id = 0;

        while page_id < num_pages{
            let read_res = hf.read_page_from_file(page_id);
            match read_res {
                Ok(mut p) => {
                    match p.add_value(&value){ 
                        None => {
                            page_id +=1; 
                        } 
                        Some(slot) => {
                            return ValueId{
                                container_id: container_id,
                                segment_id: None,
                                page_id: Some(p.deserialize_header().page_id),
                                slot_id: Some(slot),
                            }
                        } 
                    } 
                }
                _ => {
                    panic!("oh no");
                } 
            } 
        }

            let mut new = Page::new(page_id);
            hf.write_page_to_file(new);
            ValueId{ 
                container_id: container_id,
                segment_id: None,
                page_id: Some(page_id),
                slot_id: Some(0),
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
        let mut v_ids = Vec::new();
        for v in values {
            v_ids.push(self.insert_value(container_id, v, tid));
        }
        return v_ids;
    }


    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let res = self.get_page(id.container_id, id.page_id.unwrap(), tid, Permissions::ReadOnly, false);
        match res {
            None => {
                return Ok(());
            },
            Some(mut page) => {
                let del_res = page.delete_value(id.slot_id.unwrap());
                match del_res {
                    None => panic!("error in delete val"),
                    Some(_) => {
                        let ret = self.write_page(id.container_id, page, tid);
                        return ret;
                    }
                }
            }
        }
    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        let deleted = self.delete_value(id, _tid);
        match deleted {
            Err(e) => {
                Err(e)
            }
            Ok(_) => {
                let ret = self.insert_value(id.container_id, value, _tid);
                Ok(ret)
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
        let path = &mut self.storage_path.clone();
        path.push_str(&container_id.to_string());
        let buffer = PathBuf::from(path.clone());
        let mut new_hf = HeapFile::new(buffer);
        match new_hf {
            Ok(ref hf) => {
                let mut map = &mut self.hash_map.read().unwrap().clone();
                map.insert(container_id, Arc::new(new_hf.unwrap()));
            },
            Err(e) => return Err(e)
        }
        Ok(())
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
        let mut map = &mut self.hash_map.read().unwrap().clone();
        let val = map.remove(&container_id);
        if val.is_none() {
            return Err(CrustyError::CrustyError(String::from("error removing container")));
        }
        else{
            return Ok(())
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
        let mut map = self.hash_map.read().unwrap();
        let hf = map.get(&container_id).unwrap().clone();
        return HeapFileIterator::new(container_id, tid, hf);
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let map = self.hash_map.read().unwrap();
        let hf = map.get(&id.container_id).unwrap().clone();
        if id.page_id.is_none() || id.slot_id.is_none()
        {
            return Err(CrustyError::CrustyError(String::from("get_value: ValidId invalid")))
        }
        match hf.read_page_from_file(id.page_id.unwrap())
        {
            Ok(page) =>
                match page.get_value(id.slot_id.unwrap())
                {
                    Some(value) => return Ok(value),
                    None => return Err(CrustyError::CrustyError(String::from("get_value: could not get value from page")))
                },
            Err(error) => return Err(CrustyError::CrustyError(String::from("get_value: could not read page from file")))
        }

    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    fn clear_cache(&self) {
        panic!("TODO milestone hs");
    }

    /// Shutdown the storage manager. Can call drop. Should be safe to call multiple times.
    /// If temp, this should remove all stored files.
    /// If not a temp SM, this should serialize the mapping between containerID and Heapfile. 
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        panic!("TODO milestone hs");
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
        panic!("TODO milestone hs");
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
