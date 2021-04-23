extern crate csv;
use self::csv::{ByteRecord};
use std::fs::File;

pub struct FileReader {
    pub reader : csv::Reader<File>,
    pub curr_record: ByteRecord
}

impl FileReader {

    pub fn new(reader: csv::Reader<File>) -> Self {
        Self {
            reader,
            curr_record: ByteRecord::new()
        }
    }

    pub fn next_record(&mut self) -> bool {
        self.reader.read_byte_record(&mut self.curr_record).unwrap()
    }

}
