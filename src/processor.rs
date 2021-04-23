use crate::filereader::* ;
use crate::sqlops::* ;
use csv::{ReaderBuilder};

#[derive(Debug, Copy, Clone)]
pub struct Client {
    pub client_id: u16,
    pub available: f32,
    pub held: f32,
    pub total: f32,
    pub locked: bool
}

#[derive(Debug, Clone)]
pub struct Dispute {
    pub client_id: u16,
    pub event_id: u32,
    pub amount: f32,
    pub status: String
}

#[derive(Debug, Deserialize, Clone)]
//#[serde(rename_all = "PascalCase")]
pub struct Trade {
    pub Transaction_type: String ,
    pub Client_id: u16,
    pub Transaction_id: u32,
    pub Amount: Option<f32>
}

pub fn process_transactions(p: String) -> Vec<Client>{

    let mut transaction_reader = FileReader::new(ReaderBuilder::new()
        .has_headers(false)
        .from_path(p)
        .expect("Unable to process input file"));

    let mut conn = get_connection() ;

    // Load transactions
    while transaction_reader.next_record() {

        let transaction: Trade = transaction_reader.curr_record.deserialize(None).expect("Unable to deserialize record") ;
        insert_transaction(&mut conn, &transaction) ;

    }

    let output = get_output(&mut conn) ;

    conn.close().unwrap() ;
    output

}

#[cfg(test)]
mod test_process {

    use super::* ;

    pub fn process_data(file: &'static str, exp_balance: f32, exp_count: usize, exp_locked: usize) {

        build_database() ;

        let fname = String::from(file) ;
        let accounts = process_transactions(fname) ;

        let mut balance = 0.00 ;
        let count = accounts.len() ;
        let mut locked_count = 0 ;

        for a in accounts {
            balance += a.available ;
            if a.locked {
                locked_count += 1;
            }
        }

        assert_eq!(balance, exp_balance) ;
        assert_eq!(count, exp_count) ;
        assert_eq!(locked_count, exp_locked) ;
    }

    #[test]
    pub fn it_all_works() {
        process_data("data/simple.csv", 635.000, 4, 0) ;
        process_data("data/overdraft.csv", 50.000, 1, 0) ;
        process_data("data/resolved.csv", 150.000, 1, 0) ;
        process_data("data/chargedback.csv", 200.000, 1, 1) ;
    }

}