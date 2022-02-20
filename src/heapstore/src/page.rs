use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use std::convert::TryInto;
use std::mem;
use serde::{Serialize, Deserialize};
use ::slice_of_array::prelude::*;
use std::cmp::min;
use std::cmp::max;


#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub slot_id: SlotId, // 2 bytes
    pub address: u16,  // 2 bytes
    pub length: u16, // 2 byte 
}

pub struct Entries {
    pub entries: Vec<Entry> 
}


impl Entries {
    pub fn new() -> Self {
        let vec = Vec::new();
        Entries { entries: vec}
    }
}

pub struct Header {
    pub page_id: PageId, //2 bytes 
    pub count: u16, //2 byte
    pub end_free: u16,// 2 byte 
    pub open_slots: SlotId // 2 byte

}

impl Header{
    pub fn new(page_id: PageId, end_free: u16) -> Self{
        Header {page_id: page_id, count: 0, open_slots: 0, end_free: end_free }
    }

}


pub(crate) struct Page {
    /// The data for page 
    data: [u8; PAGE_SIZE],
}

/// The functions required for page
impl Page {

    // helper functions

    pub fn serialize_header(&mut self, header: &Header) {
        let mut temp: Vec<u8> = (&header.page_id.to_be_bytes()).to_vec();
        let mut count: Vec<u8> = (&header.count.to_be_bytes()).to_vec();
        let mut end_free: Vec<u8> = (&header.end_free.to_be_bytes()).to_vec();
        let mut open_slots: Vec<u8> = (&header.open_slots.to_be_bytes()).to_vec();
        temp.append(& mut count);
        temp.append(& mut end_free);
        temp.append(& mut open_slots);
        //writes directly to page 
        self.data[0..8].clone_from_slice(&temp);
    }

    pub fn deserialize_header(&self) -> Header {
        let mut dst = [0,0];
        let mut dst1= [0,0];
        let mut dst2 = [0,0];
        let mut dst3 = [0,0];
        dst.clone_from_slice(&self.data[..2]);
        dst1.clone_from_slice(&self.data[2..4]);
        dst2.clone_from_slice(&self.data[4..6]);
        dst3.clone_from_slice(&self.data[6..8]);
        Header { page_id: u16::from_be_bytes(dst), count: u16::from_be_bytes(dst1), end_free: u16::from_be_bytes(dst2), open_slots: u16::from_be_bytes(dst3)}
    }


    pub fn serialize_entry(&mut self, entry: &Entry) ->  Vec<u8> {
        let mut temp: Vec<u8> = (&entry.slot_id.to_be_bytes()).to_vec();
        let mut addr: Vec<u8> = (&entry.address.to_be_bytes()).to_vec();
        let mut length: Vec<u8> = (&entry.length.to_be_bytes()).to_vec();
        temp.append(& mut addr);
        temp.append(& mut length);
        return temp
    }

    pub fn serialize_entries(&mut self, entries: &Entries) {
        let temp = &entries.entries;
        let mut ret = Vec::new();
        temp.iter().for_each(|x| ret.append(&mut self.serialize_entry(x)));
        let header = self.deserialize_header();
        let count = header.count;
        //writes directly to page 
        self.data[8..usize::from(8+count*6)].clone_from_slice(&ret);
    } 

    pub fn deserialize_entry(&self, slotid: SlotId) -> Option<Entry> {
        let header = self.deserialize_header();
        let count = header.count; 
        if slotid >= header.count {
            return None
        }
        else {
            let mut dst = [0,0];
            let mut dst1= [0,0];
            let mut dst2 = [0,0];
            let si = usize::from(slotid*6 + 8);
            dst.clone_from_slice(&self.data[si..si+2]);
            dst1.clone_from_slice(&self.data[si+2..si+4]);
            dst2.clone_from_slice(&self.data[si+4..si+6]);
            let slot_id = u16::from_be_bytes(dst);
            let address = u16::from_be_bytes(dst1);
            let length =  u16::from_be_bytes(dst2);
            let entry : Entry = Entry {slot_id: slot_id, address:address, length:length};
            return Some(entry)
        }
    }

    pub fn deserialize_entries(&self) -> Entries {
        let header = self.deserialize_header();
        let mut vec = Vec::new();
        let count = header.count;
        if count == 0 {
            return Entries { entries : vec};
        }
        for i in 0..count {
            let entry = self.deserialize_entry(i);
            vec.push(entry.unwrap());
        }
        Entries { entries : vec}

    }

    pub fn new_entry(&mut self, length: u16) -> Entry{
        // find first empty slot 

       // println!("new entry length {}", length);
        let mut header = self.deserialize_header();
        let mut entries = self.deserialize_entries();
        let mut entries2 = self.deserialize_entries();
      //  println!("page id {} header open slots before new entry {}", header.page_id, header.open_slots);
        if header.open_slots != 0 {
            // finding open slot 
            let open_slot = entries.entries.iter().filter(|x| x.length == 0).min_by_key(|x| x.slot_id).unwrap();
            // add new entry, shift others 
            if header.page_id == 0 { 
            println!("open slot is {}", open_slot.slot_id);
            }
            let new_entry = Entry { slot_id: open_slot.slot_id, address: open_slot.address, length:length};
            let new_entry2 = Entry { slot_id: open_slot.slot_id, address: open_slot.address, length:length};
            let updated_entries = self.shift_entries_add(new_entry);
            // calculate new end free 
            let last_element = updated_entries.entries.iter().max_by_key(|x| x.slot_id).unwrap();
            let end = last_element.address - last_element.length;
            // update header 
            header.end_free = end;
            header.open_slots -= 1; 
            self.serialize_header(&header);
            self.serialize_entries(&updated_entries);
            return new_entry2
        }
        else {
            // duplicate because of reference errors 
            let new_entry = Entry { slot_id: header.count, address: header.end_free, length:length };
            let new_entry2 = Entry { slot_id: header.count, address: header.end_free, length:length };
            entries2.entries.push(new_entry);
            //updating header 

            header.end_free = &header.end_free - length; 
            header.count += 1;
            //println!("header.end_free is {}",header.end_free);
            //serializing 
            self.serialize_header(&header);
            self.serialize_entries(&entries2);
            return new_entry2;
        }
    }

    pub fn shift_entries_add(&mut self, entry:Entry) -> Entries{
        let length = entry.length;
        let slot_id = entry.slot_id;
        if self.deserialize_header().page_id == 0 { 
            println!("add slot id shifting {}", entry.slot_id);
            }
        let mut vec : Vec<Entry> = Vec::new(); 
        let entries = self.deserialize_entries();
        for e in entries.entries.iter(){
            if e.slot_id == slot_id {
                //updating previous entry with slot id with new length 
                let new = Entry { slot_id: e.slot_id, length: entry.length, address: e.address};
                vec.push(new);
            }
            else if e.slot_id > slot_id {
                // if slot_id to the left of inserted slot, update address and rewrite data with new address 
                let data = self.retrieve_data(&e).unwrap();
                let address = e.address - length;
                let new = Entry { slot_id: e.slot_id, length: e.length, address: address};
                //writing value to new address
                self.data[usize::from(address-e.length)..usize::from(address)].clone_from_slice(&data);
                vec.push(new);
            }
            else{
                // if slot_id is less than, keep the same 
                let new = Entry { slot_id: e.slot_id, length: e.length, address: e.address};
                vec.push(new);
            }
        }
        let ret = Entries{ entries: vec };
        self.serialize_entries(&ret);
        return ret;
    }


    /// Create a new page
    pub fn new(page_id: PageId) -> Self {
        //initialize header 
        let header : Header = Header::new(page_id, PAGE_SIZE.try_into().unwrap()); //struct 
        let mut temp: Vec<u8> = (&header.page_id.to_be_bytes()).to_vec();
        let mut count: Vec<u8> = (&header.count.to_be_bytes()).to_vec();
        let mut end_free: Vec<u8> = (&header.end_free.to_be_bytes()).to_vec();
        let mut open_slots: Vec<u8> = (&header.open_slots.to_be_bytes()).to_vec();
        temp.append(& mut count);
        temp.append(& mut end_free);
        temp.append(& mut open_slots);
        // add empty vector to header
        let mut data : Vec<u8> = vec![0; PAGE_SIZE -8];
        temp.append(& mut data);
        Page { data: *temp.as_array()}
    }


    /// Return the page id for a page
    pub fn get_page_id(&self) -> PageId {
        let header: Header = self.deserialize_header();
        return header.page_id
    }

    /// Attempts to add a new value to this page if there is space available.
    /// Returns Some(SlotId) if it was inserted or None if there was not enough space.
    /// Note that where the bytes are stored in the page does not matter (heap), but it
    /// should not change the slotId for any existing value. This means that
    /// bytes in the page may not follow the slot order.
    /// If a slot is deleted you should replace the slotId on the next insert.
    ///
    /// HINT: You can copy/clone bytes into a slice using the following function.
    /// They must have the same size.
    /// self.data[X..y].clone_from_slice(&bytes);

    pub fn add_value(&mut self, bytes: &[u8]) -> Option<SlotId> {
        if self.deserialize_header().page_id == 0 {
        }
        let max = self.get_largest_free_contiguous_space();
        let length : u16 = bytes.len().try_into().unwrap();
        if usize::from(length)  > max {
            None
        }
        else {
            let header = self.deserialize_header();
            //adds entry to entries, updates header count and end_free, serializes both 
            let new_entry = self.new_entry(length);
            let start_i = usize::from(new_entry.address) - usize::from(length); 
            let end_i = usize::from(new_entry.address);
            self.data[start_i..end_i].clone_from_slice(&bytes);
            let entries2 = self.deserialize_entries();
            if header.page_id == 0 {
                println!("adding entry slot_id {}, length {}", new_entry.slot_id, new_entry.length);
            }
        //     if header.page_id == 0{ 
        //     for e in entries2.entries.iter(){
        //         println!("after insert,  entries are slot id {} length {} address {} ", e.slot_id, e.length, e.address);
        //     }
        
        //     println!("done");
        // }
            Some(new_entry.slot_id)
        }
    }


    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        let entry = self.deserialize_entry(slot_id);
        if entry.is_none(){
            return None 
        }
        
        else { 
            let x = entry.unwrap();
          //  println!("retrieving entry, slot_id is {}, length is {}, address is {}", x.slot_id, x.length, x.address );
            if x.length == 0 {
                return None
            };
            return self.retrieve_data(&x);
        }
    }

    pub fn retrieve_data(&self, entry: &Entry) -> Option<Vec<u8>>  {
        //helper function to retrieve data given valid slot_id 
        let address = usize::from(entry.address);
        let length = usize::from(entry.length); 
        if length == 0{
            return None;
        }        
        let start = address-length; 
        let mut ret : Vec<u8> = vec![0; length];
        ret.clone_from_slice(&self.data[start..address]);
        return Some(ret)
    }



    /// Delete the bytes/slot for the slotId. If the slotId is not valid then return None
    /// The slotId for a deleted slot should be assigned to the next added value
    /// The space for the value should be free to use for a later added value.
    /// HINT: Return Some(()) for a valid delete
    pub fn delete_value(&mut self, slot_id: SlotId) -> Option<()> {
        let slot = self.get_value(slot_id);
        if slot.is_none(){
            return None
        }
        let entry = self.deserialize_entry(slot_id);
        let e = entry.unwrap();
        // if self.deserialize_header().page_id == 0 { 
        // println!("deleting entry slot_id {}, length {}", e.slot_id, e.length);
        // };
        self.shift_entries_del(e); 
        //updating header count and reserialize
        let mut header = self.deserialize_header();
        header.open_slots = &header.open_slots + 1;
        self.serialize_header(&header);
        let entries = self.deserialize_entries();
        let page_id = header.page_id;
        // if page_id == 0 { 
        //     for e in entries.entries.iter(){
        //         println!("after del, entries are slot id {} length {} address {} ", e.slot_id, e.length, e.address);
        //     }
        //     println!("done");
        // }

        Some(())
    }

    pub fn shift_entries_del(&mut self, entry: Entry){
        // getting info about entry to be deleted 
        if self.deserialize_header().page_id == 0 { 
            println!("del slot id shifting {}", entry.slot_id);
            }
        let length = entry.length;
        let slot_id = entry.slot_id;
        // return vec
        let mut vec = Vec::new(); 
        let entries = self.deserialize_entries();
        for e in entries.entries.iter(){
            if e.slot_id == slot_id {
                // if target entry, change length to 0 
                let new = Entry { slot_id: e.slot_id, length: 0, address: e.address};
                vec.push(new);
            }
            else if e.slot_id > slot_id {
                // for entries to the left -> shift address and rewrite data 
                let data = self.retrieve_data(&e);
                let address = e.address + length;
                let new = Entry { slot_id: e.slot_id, length: e.length, address: address};
                vec.push(new);
                if data.is_some() {
                    let d = data.unwrap();
                    self.data[usize::from(address-e.length)..usize::from(e.address + length)].clone_from_slice(&d);
                    // e.address .. e.address + length
                }
                //writing value to new address
            }
            else{
                // for entries to the right: keep it the same 
                let new = Entry { slot_id: e.slot_id, length: e.length, address: e.address};
                vec.push(new);
            }
        }
        let ret = Entries { entries: vec};
        //serialize return vector to page 
        self.serialize_entries(&ret)
    }

    /// Create a new page from the byte array.
    ///
    /// HINT to create a primitive data type from a slice you can use the following
    /// (the example is for a u16 type and the data store in little endian)
    /// u16::from_le_bytes(data[X..Y].try_into().unwrap());
    pub fn from_bytes(data: &[u8]) -> Self {
        let p_data : [u8; PAGE_SIZE]= data.try_into().expect("wrong size");
        Page { data: p_data}
    }

    /// Convert a page into bytes. This must be same size as PAGE_SIZE.
    /// We use a Vec<u8> for simplicity here.
    ///
    /// HINT: To convert a vec of bytes using little endian, use
    /// to_le_bytes().to_vec()
    pub fn get_bytes(&self) -> Vec<u8> {
        return self.data.to_vec();
    }

    /// A utility function to determine the size of the header in the page
    /// when serialized/to_bytes.
    /// Will be used by tests. Optional for you to use in your code
    #[allow(dead_code)]
    pub(crate) fn get_header_size(&self) -> usize {
        let header: Header = self.deserialize_header();
        return (8 + header.count*6).into()

    }

    /// A utility function to determine the largest block of free space in the page.
    /// Will be used by tests. Optional for you to use in your 

    #[allow(dead_code)]
    pub(crate) fn get_largest_free_contiguous_space(&self) -> usize {
        let header: Header = self.deserialize_header();
        let header_size:u16 = self.get_header_size().try_into().unwrap();
        return usize::from(header.end_free - header_size);

    }


}

/// The (consuming) iterator struct for a page.
/// This should iterate through all valid values of the page.
/// See https://stackoverflow.com/questions/30218886/how-to-implement-iterator-and-intoiterator-for-a-simple-struct
pub struct PageIter {
    page: Page,
    slot_id: SlotId    
}


/// The implementation of the (consuming) page iterator.
impl Iterator for PageIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let count = self.page.deserialize_header().count;
        if count == 0 {
            None
        }
        else {
            while self.page.get_value(self.slot_id).is_none() && self.slot_id <= count{
                self.slot_id += 1; 
            }
            let ret = self.page.get_value(self.slot_id);
            self.slot_id +=1;
            return ret;
        }

    }
}



/// The implementation of IntoIterator which allows an iterator to be created
/// for a page. This should create the PageIter struct with the appropriate state/metadata
/// on initialization.
impl IntoIterator for Page {
    type Item = Vec<u8>;
    type IntoIter = PageIter;

    fn into_iter(self) -> Self::IntoIter {
        PageIter {
            page: self,
            slot_id: 0 
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use common::testutil::init;
    use common::testutil::*;
    use common::Tuple;

    /// Limits how on how many bytes we can use for page metadata / header
    pub const FIXED_HEADER_SIZE: usize = 8;
    pub const HEADER_PER_VAL_SIZE: usize = 6;

    #[test]
    fn hs_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());
        assert_eq!(
            PAGE_SIZE - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_insert() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        let byte_len = tuple_bytes.len();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        assert_eq!(
            PAGE_SIZE - byte_len - p.get_header_size(),
            p.get_largest_free_contiguous_space()
        );
        let tuple_bytes2 = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - byte_len - byte_len,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_space() {
        init();
        let mut p = Page::new(0);
        let size = 10;
        let bytes = get_random_byte_vec(size);
        assert_eq!(10, bytes.len());
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_get_value() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Recheck
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));
    }

    #[test]
    fn hs_page_header_size_small() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let bytes = get_random_byte_vec(10);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE);
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(Some(3), p.add_value(&bytes));
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + HEADER_PER_VAL_SIZE * 4);
    }

    #[test]
    fn hs_page_header_size_full() {
        init();
        // Testing that the header is no more than 8 bytes for the header, and 6 bytes per value inserted
        let mut p = Page::new(0);
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE);
        let byte_size = 10;
        let bytes = get_random_byte_vec(byte_size);
        // how many vals can we hold with 8 bytes
        let num_vals: usize = (((PAGE_SIZE - FIXED_HEADER_SIZE) as f64
            / (byte_size + HEADER_PER_VAL_SIZE) as f64)
            .floor()) as usize;
        if PAGE_SIZE == 4096 && FIXED_HEADER_SIZE == 8 && HEADER_PER_VAL_SIZE == 6 {
            assert_eq!(255, num_vals);
        }
        for _ in 0..num_vals {
            p.add_value(&bytes);
        }
        assert!(p.get_header_size() <= FIXED_HEADER_SIZE + (num_vals * HEADER_PER_VAL_SIZE));
        assert!(
            p.get_largest_free_contiguous_space()
                >= PAGE_SIZE
                    - (byte_size * num_vals)
                    - FIXED_HEADER_SIZE
                    - (num_vals * HEADER_PER_VAL_SIZE)
        );
    }

    #[test]
    fn hs_page_no_space() {
        init();
        let mut p = Page::new(0);
        let size = PAGE_SIZE / 4;
        let bytes = get_random_byte_vec(size);
        assert_eq!(Some(0), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(1), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 2,
            p.get_largest_free_contiguous_space()
        );
        assert_eq!(Some(2), p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        //Should reject here
        assert_eq!(None, p.add_value(&bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3,
            p.get_largest_free_contiguous_space()
        );
        // Take small amount of data
        let small_bytes = get_random_byte_vec(size / 4);
        assert_eq!(Some(3), p.add_value(&small_bytes));
        assert_eq!(
            PAGE_SIZE - p.get_header_size() - size * 3 - small_bytes.len(),
            p.get_largest_free_contiguous_space()
        );
    }

    #[test]
    fn hs_page_simple_delete() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Delete slot 0
        assert_eq!(Some(()), p.delete_value(0));

        //Recheck slot 1
        let check_bytes2 = p.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(0));

        //Check that invalid slot gets None
        assert_eq!(None, p.get_value(2));

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));

        //Verify slot 0 is gone
        assert_eq!(None, p.get_value(1));
    }

    #[test]
    fn hs_page_get_first_free_space() {
        init();
        let mut p = Page::new(0);

        let _b1 = get_random_byte_vec(100);
        let _b2 = get_random_byte_vec(50);
    }

    #[test]
    fn hs_page_delete_insert() {
        init();
        let mut p = Page::new(0);
        let tuple_bytes = get_random_byte_vec(20);
        let tuple_bytes2 = get_random_byte_vec(20);
        let tuple_bytes3 = get_random_byte_vec(20);
        let tuple_bytes4 = get_random_byte_vec(20);
        let tuple_bytes_big = get_random_byte_vec(40);
        let tuple_bytes_small1 = get_random_byte_vec(5);
        let tuple_bytes_small2 = get_random_byte_vec(5);

        //Add 3 values

 

        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes2, check_bytes);
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Delete slot 1
        assert_eq!(Some(()), p.delete_value(1));
        //Verify slot 1 is gone
        assert_eq!(None, p.get_value(1));

        let check_bytes = p.get_value(0).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        let check_bytes = p.get_value(2).unwrap();
        assert_eq!(tuple_bytes3, check_bytes);

        //Insert same bytes, should go to slot 1
        assert_eq!(Some(1), p.add_value(&tuple_bytes4));

        //20 -> slot0
        // 20 -> slot1
        // 20 -> slot2
        // delete slot 1 
        // reinsert into slot 1

        let check_bytes = p.get_value(1).unwrap();
        assert_eq!(tuple_bytes4, check_bytes);

        //Delete 0
        assert_eq!(Some(()), p.delete_value(0));

        //Insert big, should go to slot 0 with space later in free block
        assert_eq!(Some(0), p.add_value(&tuple_bytes_big));

        //Insert small, should go to 3
        assert_eq!(Some(3), p.add_value(&tuple_bytes_small1));

        //Insert small, should go to new
        assert_eq!(Some(4), p.add_value(&tuple_bytes_small2));
    }

    #[test]
    fn hs_page_size() {
        init();
        let mut p = Page::new(2);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let page_bytes = p.get_bytes();
        assert_eq!(PAGE_SIZE, page_bytes.len());
    }

    #[test]
    fn hs_page_simple_byte_serialize() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 1, 2]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));
        let tuple2 = int_vec_to_tuple(vec![3, 3, 3]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        //Get bytes and create from bytes
        let bytes = p.get_bytes();
        let mut p2 = Page::from_bytes(&bytes);
        assert_eq!(0, p2.get_page_id());

        //Check reads
        let check_bytes2 = p2.get_value(1).unwrap();
        let check_tuple2: Tuple = serde_cbor::from_slice(&check_bytes2).unwrap();
        assert_eq!(tuple_bytes2, check_bytes2);
        assert_eq!(tuple2, check_tuple2);
        let check_bytes = p2.get_value(0).unwrap();
        let check_tuple: Tuple = serde_cbor::from_slice(&check_bytes).unwrap();
        assert_eq!(tuple_bytes, check_bytes);
        assert_eq!(tuple, check_tuple);

        //Add a new tuple to the new page
        let tuple3 = int_vec_to_tuple(vec![4, 3, 2]);
        let tuple_bytes3 = tuple3.get_bytes();
        assert_eq!(Some(2), p2.add_value(&tuple_bytes3));
        assert_eq!(tuple_bytes3, p2.get_value(2).unwrap());
        assert_eq!(tuple_bytes2, p2.get_value(1).unwrap());
        assert_eq!(tuple_bytes, p2.get_value(0).unwrap());
    }

    #[test]
    fn hs_page_iter() {
        init();
        let mut p = Page::new(0);
        let tuple = int_vec_to_tuple(vec![0, 0, 1]);
        let tuple_bytes = serde_cbor::to_vec(&tuple).unwrap();
        assert_eq!(Some(0), p.add_value(&tuple_bytes));

        let tuple2 = int_vec_to_tuple(vec![0, 0, 2]);
        let tuple_bytes2 = serde_cbor::to_vec(&tuple2).unwrap();
        assert_eq!(Some(1), p.add_value(&tuple_bytes2));

        let tuple3 = int_vec_to_tuple(vec![0, 0, 3]);
        let tuple_bytes3 = serde_cbor::to_vec(&tuple3).unwrap();
        assert_eq!(Some(2), p.add_value(&tuple_bytes3));

        let tuple4 = int_vec_to_tuple(vec![0, 0, 4]);
        let tuple_bytes4 = serde_cbor::to_vec(&tuple4).unwrap();
        assert_eq!(Some(3), p.add_value(&tuple_bytes4));

        let tup_vec = vec![
            tuple_bytes.clone(),
            tuple_bytes2.clone(),
            tuple_bytes3.clone(),
            tuple_bytes4.clone(),
        ];
        let page_bytes = p.get_bytes();

        // Test iteration 1
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes3.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(None, iter.next());
        //Check another way
        let p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(tuple_bytes.clone()), p.get_value(0));

        for (i, x) in p.into_iter().enumerate() {
            assert_eq!(tup_vec[i], x);
        }

        let p = Page::from_bytes(&page_bytes);
        let mut count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 4);

        //Add a value and check
        let mut p = Page::from_bytes(&page_bytes);
        assert_eq!(Some(4), p.add_value(&tuple_bytes));
        //get the updated bytes
        let page_bytes = p.get_bytes();
        count = 0;
        for _ in p {
            count += 1;
        }
        assert_eq!(count, 5);

        //Delete
        let mut p = Page::from_bytes(&page_bytes);
        println!("delete value 2");
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}
