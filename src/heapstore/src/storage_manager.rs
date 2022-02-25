use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use crate::buffer_pool::BufferPool;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::path::{PathBuf, Path};
use std::sync::{Arc, RwLock};


/// The StorageManager struct
pub struct StorageManager {
    pub s_path: String,
    indices: Arc<RwLock<HashMap<ContainerId, usize>>>,
    hfs: Arc<RwLock<Vec<Arc<HeapFile>>>>,
    bp: Arc<RwLock<BufferPool>>,
    is_temp: bool,
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
        // helper function that omits optional variables
        self.get_page_helper(container_id, page_id)
    }

    pub(crate) fn get_page_helper(
        &self,
        container_id: ContainerId,
        page_id: PageId,
    ) -> Option<Page> {
        let mut bp = self.bp.write().unwrap();
        let res = bp.get_page(container_id, page_id);
        if res.is_none(){
            let indices = self.indices.read().unwrap();
            let heapfiles = self.hfs.write().unwrap();
            let index = indices.get(&container_id);
            if index.is_none(){
                return None;
            }
            else{
                let idx = index.unwrap();
                match heapfiles[*idx].read_page_from_file(page_id) {
                    Ok(page) => {
                        bp.insert_page(&page, container_id);
                        return Some(page);
                    }
                    Err(_) => {
                        return None;
                    }
                }
            }
        }
        else{
            let page= res.unwrap();
            return Some(page)
        }
    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        self.write_page_helper(container_id, page)
    }

    //helper function for write page 
    pub(crate) fn write_page_helper(
        &self,
        container_id: ContainerId,
        page: Page,
    ) -> Result<(), CrustyError> {
        let indices = self.indices.read().unwrap();
        let res = indices.get(&container_id);
        if res.is_none(){
            return Err(CrustyError::CrustyError("container id invalid".to_string()));
        }
        else {
            let idx = res.unwrap();
            let mut bp = self.bp.write().unwrap();
            bp.write_page(&page, container_id);
            let heapfiles = self.hfs.write().unwrap();
            let ret = heapfiles[*idx].write_page_to_file(page);
            return ret;
        }

    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let indices = self.indices.read().unwrap();
        let ret = indices.get(&container_id);
        if ret.is_none(){
            panic!("containerid {} is invalid", container_id);
        }
        else {
            let idx = ret.unwrap();
            let heapfiles = self.hfs.write().unwrap();
            let ret = heapfiles[*idx].num_pages();
            return ret;
        }
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
        let create = fs::create_dir(&storage_path);
        match create {
            Ok(()) => {
                let ret =        
                 StorageManager {
                    s_path: storage_path,
                    indices: Arc::new(RwLock::new(HashMap::new())),
                    hfs: Arc::new(RwLock::new(Vec::new())),
                    bp: Arc::new(RwLock::new(BufferPool::new())),
                    is_temp: false,
                };
                return ret;
            },
            Err(e)=> panic!("create dir didn't work")
        }

    }
    /// Create a new storage manager for testing. If this creates a temporary directory it should be cleaned up
    /// when it leaves scope.
    fn new_test_sm() -> Self {
        let storage_path = gen_random_dir().to_string_lossy().to_string();
        let create = fs::create_dir(&storage_path);
        match create {
            Err(e)=> panic!("create dir didn't work"),
            Ok(()) => {
                let ret =         StorageManager {
                    s_path: storage_path,
                    indices: Arc::new(RwLock::new(HashMap::new())),
                    hfs: Arc::new(RwLock::new(Vec::new())),
                    bp: Arc::new(RwLock::new(BufferPool::new())),
                    is_temp: true,
                };
                return ret;
            }
        }
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
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        }

        let mut buffer_pool = self.bp.write().unwrap();
        let indices = self.indices.read().unwrap();
        let mut value_id = ValueId::new(container_id);
        let heapfiles = self.hfs.write().unwrap();
        let ret =  indices.get(&container_id);
        if ret.is_none(){
            panic!("containerID {} is invalid", container_id);
        }
        else{
            let idx = ret.unwrap();
            let length = value.len();
            let p_ids = heapfiles[*idx].find_pages_to_insert(length);
            let page_iter = p_ids.iter();
            for &page_id in page_iter{
                let mut page;
                let ret = buffer_pool.get_page(container_id, page_id);
                if ret.is_none(){
                    let ret1 = heapfiles[*idx].read_page_from_file(page_id);
                    match ret1 {
                        Err(_) => {
                            panic!("fails to read page from file");
                        },
                        Ok(p) => {
                            buffer_pool.insert_page(&p, container_id);
                            page = p
                        }

                    }
                }
                else{
                    let p = ret.unwrap();
                    page = p;
                }
                let add_value = page.add_value(&value);
                if add_value.is_some() {
                    let slot_id = add_value.unwrap();
                    buffer_pool.write_page(&page, container_id);
                    let write = heapfiles[*idx].write_page_to_file(page);
                    match write {
                        Err(e) => panic!("write failed"),
                        Ok(()) => {
                            value_id.slot_id = Some(slot_id);
                            value_id.page_id = Some(page_id as PageId);
                            return value_id;
                        }
                    }
                }
            };

            let num_pages = heapfiles[*idx].num_pages();
            let mut page = Page::new(num_pages);
            let add_value = page.add_value(&value);
            if add_value.is_some(){
                let slot_id = add_value.unwrap();
                buffer_pool.write_page(&page, container_id);
                let write = heapfiles[*idx].write_page_to_file(page);
                match write {
                    Err(e) => panic!("write failed"),
                    Ok(()) => {
                        value_id.slot_id = Some(slot_id);
                        value_id.page_id = Some(num_pages);
                        return value_id;
                    }
                }
            }
            else{
                panic!("add value failed");
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
        let mut val_ids = Vec::new();
        for val in values {
            val_ids.push(self.insert_value(container_id, val, tid));
        }
        return val_ids;
    }


    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let p_id = id.page_id.unwrap();
        let ret = self.get_page_helper(id.container_id, p_id);
        if ret.is_none(){
            return Ok(())
        }
        else {
            let mut page = ret.unwrap();
            let slot_id = id.slot_id.unwrap();
            let page_delete = page.delete_value(slot_id);
            if page_delete.is_none(){
                return Err(CrustyError::IOError("couldn't delete".to_string()));
            }
            else{
                return self.write_page_helper(id.container_id, page);
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
        let delete = self.delete_value(id, _tid);
        match delete {
            Ok(_) => {
                let insert = self.insert_value(id.container_id, value, _tid);
                return Ok(insert);
            }
            Err(err) => {
                return Err(CrustyError::IOError("couldn't update".to_string()));
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
        let mut indices = self.indices.write().unwrap();
        if !indices.contains_key(&container_id) {
            let mut path = PathBuf::from(&self.s_path);
            path.push(container_id.to_string());
            path.set_extension("hf");
            fs::create_dir_all(self.s_path.clone())?;
            match HeapFile::new(path) {
                Ok(hf) => {
                    let mut hfiles = self.hfs.write().unwrap();
                    let new = Arc::new(hf);
                    hfiles.push(new);
                    indices.insert(container_id, hfiles.len() - 1);
                    return Ok(());
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
        else {
            return Ok(());
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
        panic!("to do");
    }



    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let indices = self.indices.read().unwrap();
        let heapfiles = self.hfs.write().unwrap();
        let ret = indices.get(&container_id);
        if ret.is_none(){
            panic!("containerId not found in get iterator");
        }
        else {
            let idx = ret.unwrap();
            let new  = HeapFileIterator::new(
                container_id,
                tid,
                heapfiles[*idx].clone(),
                self.bp.clone(),
            );
            return new
        }
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let p_id = id.page_id.unwrap();
        let p = self.get_page_helper(id.container_id,p_id );
        if p.is_none(){
            return Err(CrustyError::CrustyError("invalid valueid".to_string()));
        }
        else {
            let page = p.unwrap();
            let slot_id = id.slot_id.unwrap();
            let val = page.get_value(slot_id);
            if val.is_none(){
                return Err(CrustyError::CrustyError("valueID not found".to_string()));
            }
            else{
                let ret = val.unwrap();
                return Ok(ret)
            }
        }
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    fn transaction_finished(&self, tid: TransactionId) {
        panic!("TODO milestone tm");
    }

    /// Testing utility to reset all state associated the storage manager.
    fn reset(&self) -> Result<(), CrustyError> {
        panic!("to do");
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
        drop(self);
        if self.is_temp {
            fs::remove_dir_all(&self.s_path);
        }  
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
        if self.is_temp {
            fs::remove_dir_all(&self.s_path);
        }
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
