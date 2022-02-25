use crate::page::Page;
use common::ids::{ContainerId, PageId};
use rand::Rng;
use std::collections::HashMap;

/// The Frame struct
pub struct Frame {
    pub dirty: bool,
    pub c_id: Option<ContainerId>,
    pub page: Option<Page>,
    pub p_id: Option<PageId>,
}

impl Frame {
    pub(crate) fn new(p: Option<Page>) -> Self {
        let mut frame = Frame {
            c_id: None,
            page: None,
            p_id: None,
            dirty: false,
        };
        if p.is_some(){
            frame.page = Some(p.unwrap().clone());
            return frame;
        }
        else {
            return frame;
        }
    }

    pub(crate) fn add_page(&mut self, page: &Page, c_id: ContainerId) {
        self.p_id = Some(page.get_page_id());
        self.c_id = Some(c_id);
        self.page = Some(page.clone());
    }
}

/// bp struct 
pub(crate) struct BufferPool {
    pub indices: HashMap<(ContainerId, PageId), usize>,
    next_frame: usize,
    pub frames: Vec<Frame>,
}

impl BufferPool {
    pub(crate) fn new() -> Self {
        let fs = Vec::new();
        let is = HashMap::new();
        let mut new = BufferPool {

            frames: fs,
            indices: is,
            next_frame: 0,
        };
        let mut i = 0;
        while i < 50{
            let new_f = Frame::new(None);
            new.frames.push(new_f);
            i+=1
        }
        return new
    }

    /// write function for pages already in buffer pool
    pub(crate) fn write_page(&mut self, page: &Page, c_id: ContainerId) {
        let i = self.indices.get(&(c_id, page.get_page_id()));
        if i.is_none(){
            self.insert_page(page,c_id);
        }
        else{
            let idx = i.unwrap();
            self.frames[*idx].page = Some(page.clone());
        }
    }

    /// retrieves page if it's in the buffer pool
    pub(crate) fn get_page(&self, c_id: ContainerId, p_id: PageId) -> Option<Page> {
        let ret = self.indices.get(&(c_id, p_id));
        if ret.is_none(){
            return None;
        }
        else {
            let i = ret.unwrap();
            let frame = &self.frames[*i];
            let fp = &frame.page;
            if fp.is_none(){
                panic!("fame is empty");
            }
            else{
                return fp.clone();
            }

        }
    }

    /// inserts page into buffer pool , returns index
    pub(crate) fn insert_page(&mut self, page: &Page, c_id: ContainerId) -> usize {
        let mut i = self.next_frame;
        // if there are empty frames
        if i == 50 {
            i = self.evict();
        } else {
            self.next_frame += 1;
        }
        let i1 = c_id;
        let i2 = page.get_page_id();
        self.indices.insert((i1,i2), i);
        self.frames[i].add_page(page, c_id);
        i 
    }


    //evicts random page from pool and returns index
    pub(crate) fn evict(&mut self) -> usize {
        if self.next_frame == 50 {
            panic!("no need to call evict sweetheart")
        }
        let mut i = 0; 
        while i < 50 {
            if self.frames[i].dirty {
                i+=1;
            }
            else {
                let i1 = self.frames[i].c_id.unwrap();
                let i2 = self.frames[i].p_id.unwrap();
                self.indices.remove(&(i1, i2));
                let new = Frame::new(None);
                self.frames[i] = new;
                return i

            }
        }
        return i
    }
}