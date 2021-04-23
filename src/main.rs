#![allow(non_snake_case)]

extern crate serde ;
#[macro_use]
extern crate serde_derive;
extern crate rusqlite ;

use std::env;
use std::process ;
use std::path::Path;

use crate::processor::*;
use crate::sqlops::{build_database};

mod filereader;
mod processor;
mod sqlops ;

fn main() {

    let args: Vec<String> = env::args().collect();

    if args.len() == 1 {
        println!("** Missing parameter **") ;
        println!("Please enter the path of the input file and and specify the output as follows (example):");
        println!("'$ cargo run -- transactions.csv' to display the output on the screen");
        println!("'$ cargo run -- transactions.csv > accounts.csv' to write the output to a file");
        println!() ;
        process::exit(1);
    }

    let fname = args[1].clone() ;
    if Path::new(fname.as_str()).exists() {
        eprintln!("Loading file '{}'", fname);
    } else {
        eprintln!("File '{}' not found", fname) ;
        process::exit(1) ;
    }

    eprintln!("Building databases") ;
    build_database();

    let accounts = process_transactions(fname);
    eprintln!("Processed transactions for {} accounts", accounts.len()) ;
    eprintln!("Outputting accounts ..") ;
    output_account(accounts) ;
    eprintln!("Finished generating account data") ;

}

fn output_account(accounts: Vec<Client>) {
    println!("client,available,held,total,locked") ;
    for acct in accounts {
        println!("{},{:.4},{:.4},{:.4},{}",acct.client_id, acct.available, acct.held, acct.total, acct.locked) ;
    }
}