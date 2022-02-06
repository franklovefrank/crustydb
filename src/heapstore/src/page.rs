use common::ids::{PageId, SlotId};
use common::PAGE_SIZE;
use std::convert::TryInto;
use std::mem;
use serde::{Serialize, Deserialize};
use ::slice_of_array::prelude::*;
use std::cmp::min;
use std::cmp::max;


// inserting values: need to update so min of slot_id in entries s.t. (open == true ) && (length > val.length) 
// if false, then new slot 
//delete values: change length to 0 
// have to recalculate length -- > find entry with slot_id = id + 1


#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub slot_id: SlotId, // 2 bytes
    pub address: u16,  // 2 bytes
    pub length: u16, // 2 byte 
    //need to reserialize
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


/// The struct for a page. Note this can hold more elements/meta data when created,
/// but it must be able to be packed/serialized/marshalled into the data array of size
/// PAGE_SIZE. In the header, you are allowed to allocate 8 bytes for general page metadata and
/// 6 bytes per value/entry/slot stored. For example a page that has stored 3 values, can use
/// up to 8+3*6=26 bytes, leaving the rest (PAGE_SIZE-26 for data) when serialized.
/// You do not need reclaim header information for a value inserted (eg 6 bytes per value ever inserted)
/// The rest must filled as much as possible to hold values.
pub(crate) struct Page {
    /// The data for data
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


    pub fn serialize_entry(entry: &Entry) ->  Vec<u8> {
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
        temp.iter().for_each(|x| ret.append(&mut Page::serialize_entry(x)));
        let header = self.deserialize_header();
        let count = header.count;
        self.data[8..usize::from(8+count*6)].clone_from_slice(&ret);
    } 

    pub fn deserialize_entry(&self, start: usize,end:usize, slice: &Vec<u8>) -> Entry {
        let mut dst = [0,0];
        let mut dst1= [0,0];
        let mut dst2 = [0,0];
        dst.clone_from_slice(&slice[start..start+2]);
        dst1.clone_from_slice(&slice[start+2..start+4]);
        dst2.clone_from_slice(&slice[start+4..start+6]);
        let slot_id = u16::from_be_bytes(dst);
        let address = u16::from_be_bytes(dst1);
        let length =  u16::from_be_bytes(dst2);
        let entry : Entry = Entry {slot_id: slot_id, address:address, length:length};
        entry

    }
    pub fn deserialize_entries(&self) -> Entries {
        let header = self.deserialize_header();
        let mut vec = Vec::new();
        let count = usize::from(header.count);
        if count == 0 {
            return Entries { entries : vec};
        }
        let slice_size : usize = count * 6; 
        let end: usize = 8 + usize::from(6 * count);
        let mut slice = vec![0; slice_size];
        slice.clone_from_slice(&self.data[8..usize::from(end)]);

        for i in 0..count {
            let si = i*6;
            let ei = i*6 + 6; 
            let entry = self.deserialize_entry(si, ei,&slice);
            vec.push(entry)
        }
        Entries { entries : vec}

    }

    pub fn new_entry_header(&mut self, length: u16) -> Entry{
        // find first empty slot 
        let mut header = self.deserialize_header();
        let mut entries = self.deserialize_entries();
        let entries_t = self.deserialize_entries();
        if header.open_slots != 0 {
            let slot = entries_t.entries.iter().filter(|x| x.length == 0).min_by_key(|x| x.slot_id).unwrap();
            let slot_id = slot.slot_id;
            let new_entry = Entry { slot_id: slot_id, address: slot.address, length:length};
            println!("open slot is {} length is {} address is {}", slot_id, length, slot.address);
            let filtered = entries.entries.iter().filter(|x| x.slot_id != slot.slot_id);
            let mut ret = Vec::<Entry>::new();
            let mut difference : u16 = 0;
            if slot.length >= new_entry.length {
                difference = slot.length - new_entry.length;
                for f in filtered {
                    if f.slot_id > slot_id {
                        let address = f.address + difference;
                        let temp = Entry {slot_id: f.slot_id, address: address, length:f.length};
                        ret.push(temp);
                    }
                    else {
                        let address = f.address;
                        let temp = Entry {slot_id: f.slot_id, address: address, length:f.length};
                        ret.push(temp);
                    }
                }
                
            }
            else {
                difference = new_entry.length - slot.length;
                for f in filtered {
                    if f.slot_id > slot_id {
                        let address = f.address - difference;
                        let temp = Entry {slot_id: f.slot_id, address: address, length:f.length};
                        ret.push(temp);
                    }
                    else {
                        let address = f.address;
                        let temp = Entry {slot_id: f.slot_id, address: address, length:f.length};
                        ret.push(temp);
                    }
                }
            }
 
        
            let ret_entry = Entry { slot_id: slot_id, address: slot.address, length:length};
            ret.insert(slot_id.into(), ret_entry);
            let last_element = ret.iter().max_by_key(|x| x.slot_id).unwrap();
            let end = last_element.address - last_element.length;
            header.end_free = end;
            header.open_slots -= 1; 
            self.serialize_header(&header);
            let new_entries = Entries { entries : ret};
            self.serialize_entries(&new_entries);     
           // println!("(if statement) new entry added slot_id {} address {} length {} ", new_entry.slot_id, new_entry.address, new_entry.length);
            println!("header count is {}", header.count);
            let ret_entry_2 = Entry { slot_id: slot_id, address: slot.address, length:length};
            return ret_entry_2;
        }
        else {
            let new_entry = Entry { slot_id: header.count, address: header.end_free, length:length };
            let new_entry2 = Entry { slot_id: header.count, address: header.end_free, length:length };
            let new_entry3 = Entry { slot_id: header.count, address: header.end_free, length:length };
            println!("else open slot is {} length is {} address is {}", header.count, length, header.end_free);
            entries.entries.push(new_entry);
            header.end_free = &header.end_free - length; 
            header.count += 1;
            self.serialize_header(&header);
            self.serialize_entries(&entries);
            println!("(if statement) new entry added slot_id {} address {} length {} ", new_entry3.slot_id, new_entry3.address, new_entry3.length);
            println!("header count is {}", header.count);
            return new_entry2;
        }
    }


    /// Create a new page
    pub fn new(page_id: PageId) -> Self {
        let header : Header = Header::new(page_id, PAGE_SIZE.try_into().unwrap()); //struct 
        let mut temp: Vec<u8> = (&header.page_id.to_be_bytes()).to_vec();
        let mut count: Vec<u8> = (&header.count.to_be_bytes()).to_vec();
        let mut end_free: Vec<u8> = (&header.end_free.to_be_bytes()).to_vec();
        let mut open_slots: Vec<u8> = (&header.open_slots.to_be_bytes()).to_vec();
        temp.append(& mut count);
        temp.append(& mut end_free);
        temp.append(& mut open_slots);
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
        let max = self.get_largest_free_contiguous_space();
        let length : u16 = bytes.len().try_into().unwrap();
        if usize::from(length)  > max {
            None
        }
        else {
            let header = self.deserialize_header();
            let count = header.count; 
            let entries = self.deserialize_entries();
            //adds entry to entries, updates header count and end_free, serializes both 
            let new_entry = self.new_entry_header(length);
            let start_i = usize::from(new_entry.address) - usize::from(length); 
            let end_i = usize::from(new_entry.address);
            self.data[start_i..end_i].clone_from_slice(&bytes);
            println!("added value {}", new_entry.slot_id);
            let entries2 = self.deserialize_entries();
            for i in entries2.entries.iter(){
                println!("in add_value slot id {} length {} address {} ", i.slot_id, i.length, i.address);
            };
            println!("added entry slot id {} length {} address {} ", new_entry.slot_id, new_entry.length, new_entry.address);
            Some(new_entry.slot_id)
        }
    }


    /// Return the bytes for the slotId. If the slotId is not valid then return None
    pub fn get_value(&self, slot_id: SlotId) -> Option<Vec<u8>> {
        let entries: Entries = self.deserialize_entries();
        let mut entry = entries.entries.iter().find(|x| x.slot_id==slot_id);
        if entry.is_none(){
            return None
        }
        else { 
            let x = entry.unwrap();
           // println!("entry is slot_id {} address {} length {}", x.slot_id, x.address, x.length);
            if x.length == 0 {
                return None
            };
            return self.retrieve_data(x)
        }
    }

    pub fn retrieve_data(&self, entry: &Entry) -> Option<Vec<u8>>  {
        let address = usize::from(entry.address);
        let length = usize::from(entry.length); 
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
        println!("deleting slot {}", slot_id);
        if slot.is_none(){
            return None
        }
        //updating entry 
        let e_len_loc = usize::from(8 + (slot_id * 6) + 4);
        let leng = [0;2];
        &self.data[e_len_loc..e_len_loc+2].clone_from_slice(&leng);


        //updating header count
        let mut header = self.deserialize_header();
        header.open_slots = &header.open_slots + 1;
        self.serialize_header(&header);
        let entries = self.deserialize_entries();
        for i in entries.entries.iter(){
            println!("in delete_value slot id {} length {} address {} ", i.slot_id, i.length, i.address);
        };
        Some(())

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
        let entries = self.deserialize_entries();
        let max_open_slot = entries.entries.iter().filter(|x| x.length == 0).max_by_key(|x| x.length);
        match max_open_slot {
            Some(x) => return max(usize::from(x.length), usize::from(header.end_free - header_size)),
            None => return usize::from(header.end_free - header_size)
        }

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
        println!("{} slot_id in iterator", self.slot_id);
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
        p.delete_value(2);
        let mut iter = p.into_iter();
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(Some(tuple_bytes2.clone()), iter.next());
        assert_eq!(Some(tuple_bytes4.clone()), iter.next());
        assert_eq!(Some(tuple_bytes.clone()), iter.next());
        assert_eq!(None, iter.next());
    }
}
