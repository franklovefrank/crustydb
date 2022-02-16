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
    hf_map: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>
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
        let heapfiles = self.hf_map.read().unwrap().clone();
        if !heapfiles.contains_key(&container_id){
            None
        } else {
            let heapfile = heapfiles[&container_id].clone();
            let ret_page = HeapFile::read_page_from_file(&heapfile, page_id);
            Some(ret_page.unwrap())
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
        let hf = map.get(&container_id);
        match hf {
            Ok(()) => {
               res = hf.unwrap();
               write_page_to_file(res); 
               return Ok(());
            },
            Err(e) => return Err(CrustyError::CrustyError(String::from(
                "write_page: could not write page to file"))),
        }
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        /* Get a pointer to the heapfiles vector */
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
    //     let map = &*self.hash_map.read().unwrap();
    //     if map.contains_key(&container_id){
    //         let heap_file = map.get(&container_id).unwrap();
    //         let w_count = heap_file.write_count.load(Ordering::Relaxed);
    //         let r_count = heap_file.read_count.load(Ordering::Relaxed);
    //         (r_count, w_count)
    //     } else {
    //         (0,0) 
    //     }
    }
}

/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk)
    fn new(storage_path: String) -> Self {
        let new_sm = StorageManager{hf_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: false};
        new_sm
    }
    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        let new_sm = StorageManager{hf_map: Arc::new(RwLock::new(HashMap::new())), storage_path: storage_path, is_temp: true};
        new_sm
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        /* Panic if value contains more than PAGE_SIZE bytes */
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }
        let map = self.hf_map.read().unwrap().clone();
        let hf = map.get(&container_id);
        if hf.is_none(){
            panic!("heapfile doesn't exist")
        }
        let heapfile = hf.unwrap();
        let num_pages = heapfile.num_pages();
        let mut page_id = 0;


        while page_id < num_pages{
            match heapfile.read_page_from_file(page_id){ 
                Ok(mut page) => {
                    match page.add_value(&value){ 
                        Some(slot_id) => {
                            return ValueId{
                                container_id: container_id,
                                segment_id: None,
                                page_id: Some(page.header.page_id),
                                slot_id: Some(slot_id),
                            }
                        } // closes Some(slot_id)
                        None => {
                            // go to the next page
                            page_id +=1; 
                        } // closes None
                    } // closes match page.add_value(&value)
                } // closes Ok(mut page)
                _ => {
                    panic!("doesn't work");
                } // closes _ 
            } //closes match.hf.read_page_from_file(page_id)
        }

        let mut new_page = Page::new(page_id);
        heapfile.write_page_to_file(new_page);
        let new_val_id = ValueId{ 
            container_id: container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: Some(0),
        };
        return new_val_id;

    };


    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        panic!("TODO milestone hs");
    }


    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        panic!("TODO milestone hs");
        // /* Get the heapfile with the provided container_id */
        // let heapfiles = self.heapfiles_lock.clone().write().unwrap().clone();
        // let heapfile = self.lookup_hf(heapfiles, id.container_id).clone();

        // /* Make sure that neither page_id nor slot_id are None values */
        // if id.page_id.is_none() || id.slot_id.is_none()
        // {
        //     return Err(CrustyError::CrustyError(String::from("ERROR")))
        // }

        // /* Try to read the page from file with the provided page_id */
        // match heapfile.read_page_from_file(id.page_id.unwrap())
        // {
        //     Ok(mut page) =>
        //         /* Try to delete the value from the page */
        //         match page.delete_value(id.slot_id.unwrap())
        //         {
        //             Some(()) => return Ok(()),
        //             None => Ok(())
        //         },
        //     Err(error) => Ok(())
        // }
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
        /* Try to delete a value */
        panic!("TODO milestone hs");
        // match self.delete_value(id, _tid)
        // {
        //     /* delete_value succeeded: insert the new value into the heapfile */
        //     Ok(()) => return Ok(self.insert_value(id.container_id, value, _tid)),

        //     /* delete_value failed: return a CrustyError */
        //     Err(error) => return Err(error)
        // }
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
        {
            let mut map = self.hash_map.read().unwrap();

            let path = &mut self.storage_path.clone();
            path.push_str(&container_id.to_string());

            let mut new_hf = HeapFile::new(container_id).unwrap();
            println!("container_id: {:?}", container_id);
            map.insert(container_id, Arc::new(new_hf));
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
        panic!("TODO milestone hs");
    }



    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        panic!("TODO milestone hs");
    }
    //     /* Get the heapfile pointer given the provided container_id */
    //     let heapfiles = self.heapfiles_lock.clone().write().unwrap().clone();
    //     let heapfile = self.lookup_hf(heapfiles, container_id).clone();

    //     /* Return a new iterator */
    //     return HeapFileIterator::new(container_id, tid, heapfile);
    // }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        panic!("TODO milestone hs");
        // /* Get the heapfile pointer given the provided container_id */
        // let heapfiles = self.heapfiles_lock.clone().read().unwrap().clone();
        // let heapfile = self.lookup_hf(heapfiles, id.container_id).clone();

        // /* Make sure that neither page_id nor slot_id are None values */
        // if id.page_id.is_none() || id.slot_id.is_none()
        // {
        //     return Err(CrustyError::CrustyError(String::from("get_value: ValidId invalid")))
        // }

        // /* Try to read a page from heapfile given the page_id */
        // match heapfile.read_page_from_file(id.page_id.unwrap())
        // {
        //     /* read_page_from_file succeeded */
        //     Ok(page) =>
        //         match page.get_value(id.slot_id.unwrap())
        //         {
        //             Some(value) => return Ok(value),
        //             None => return Err(CrustyError::CrustyError(String::from("get_value: could not get value from page")))
        //         },

        //     /* read_page_from_file failed */
        //     Err(error) => return Err(CrustyError::CrustyError(String::from("get_value: could not read page from file")))
        // }
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
        // drop(self);

        // if self.is_temp == true
        // {
        //     let heapfiles = self.heapfiles_lock.write().unwrap();
        //     for i in 0..heapfiles.len()
        //     {
        //         let string = format!("{}{}", self.storage_path, i);
        //         match fs::remove_file(string)
        //         {
        //             Ok(()) => (),
        //             Err(error) => panic!("shutdown: could not remove temporary file")
        //         }
        //     }
        // }
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
        println!("just making sure it doesn't get here");
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
