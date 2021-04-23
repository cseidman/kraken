use rusqlite::*;
use std::process ;
use std::fs::File;
use std::io::Read;

use crate::processor::{Trade, Client, Dispute};

pub fn get_connection() -> Connection {
   Connection::open("transact_db").expect("Unable to establish SQLite connection")
}

pub fn read_file(fname: &str) -> Option<String> {
    // Open the file in read-only mode.
    match File::open(fname) {
        Ok(mut file) => {
            let mut text = String::new();
            file.read_to_string(&mut text).unwrap();
            return Some(text) ;
        },
        Err(error) => {
            eprintln!("Error opening file {}: {}", fname, error);
        },
    }
    None
}

// Rebuilds all the objects in the database from scratch
pub fn build_database() {

    let conn = get_connection() ;

    let sql =  read_file("sql/transact.sql") ;
    if sql.is_none() {
        eprintln!("Unable to parse sql file") ;
        panic!() ;
    }
    let res = conn.execute_batch(sql.unwrap().as_str()) ;
    if res.is_err() {
        eprintln!("SQL build failed") ;
        process::exit(1);
    } else {
        eprintln!("SQL build succeeded") ;
    }
}

pub fn get_trade_transaction(conn: &Transaction, transaction_id: u32) -> Option<Trade> {

    let mut pstmt = conn.prepare("select * from trade_event where event_type in ('withdrawal','deposit') and event_id = ?").unwrap();
    let trn = pstmt.query_map([transaction_id], |row| {
        Ok( Trade {
            Transaction_type: row.get(2)?,
            Client_id: row.get(1)?,
            Transaction_id:  row.get(0)?,
            Amount: row.get(3).unwrap(),
        })
    }).unwrap();

    let mut vtrans: Vec<Trade> = Vec::new() ;
    for t in trn {
        vtrans.push(t.unwrap());
    }
    if vtrans.len()== 0 {
        return None ;
    }
    Some(vtrans[0].clone())
}

pub fn get_output(conn: &Connection) -> Vec<Client> {

    let mut pstmt = conn.prepare("select * from client_account").unwrap();
    let trn = pstmt.query_map([], |row| {
        Ok( Client {
            client_id: row.get(0)?,
            available: row.get(1)?,
            total: row.get(2)?,
            held: row.get(3)?,
            locked: row.get(4)?
        })
    }).unwrap();

    let mut vcli: Vec<Client> = Vec::new() ;
    for t in trn {
        vcli.push(t.unwrap());
    }
    vcli
}

pub fn get_account(conn: &Connection, client_id: u16) -> Option<Client> {
    let mut pstmt = conn.prepare("select * from client_account where client_id = ?").unwrap();
    let trn = pstmt.query_map([client_id], |row| {
        Ok( Client {
            client_id: row.get(0)?,
            available: row.get(1)?,
            total: row.get(2)?,
            held: row.get(3)?,
            locked: row.get(4)?
        })
    }).unwrap();

    let mut vcli: Vec<Client> = Vec::new() ;
    for t in trn {
        vcli.push(t.unwrap());
    }
    if vcli.len()== 0 {
        return None ;
    }
    Some(vcli[0])
}
/**
Gets open disputes only - no resolved or charged back ones
*/
pub fn get_dispute(conn: &Transaction, client_id: u16, transaction_id: u32 ) -> Option<Dispute>{

    let mut pstmt = conn.prepare("select * from dispute where client_id = ? and event_id = ? and status = 'disputed'").unwrap();
    let trn = pstmt.query_map(params![client_id, transaction_id], |row| {
        Ok( Dispute {
                client_id: row.get(0)?,
                event_id: row.get(1)?,
                amount: row.get(2)?,
                status: row.get(3)?
            })
    }).expect("Unable to retrieve open dispute data");

    let mut vdisp: Vec<Dispute> = Vec::new() ;
    for t in trn {
        vdisp.push(t.unwrap());
    }
    if vdisp.len()== 0 {
        return None ;
    }
    Some(vdisp[0].clone())

}

pub fn insert_transaction(conn: &mut Connection, t: &Trade) {

    let opt_acct = get_account(conn, t.Client_id) ;

    // If the account is blocked, then nothing can be done with it - just go on to the next one
    if opt_acct.is_some() && opt_acct.unwrap().locked {
        eprintln!("Client account {} is locked. We won't process transaction {}", t.Client_id, t.Transaction_id) ;
        return;
    }

    let trx = conn.transaction().unwrap() ;

    // First update the account:
    // If there is no client .. we open an account using this transaction (if it's a deposit)
    if opt_acct.is_none() {
        // open the account with the first deposit
        if t.Transaction_type == "deposit" {
            trx.execute("insert into client_account (client_id, available, total, held, locked) values (?1, ?2, ?3, ?4, ?5)", params![t.Client_id, t.Amount, t.Amount, 0, false])
                .expect("Failed to open new account");
        } else {
            // This shouldn't happen .. but let's warn anyway since we can't open an account with anything other than a deposit
            // in that case, we discard the transaction
            eprintln!("Account # {} not open to process {} in transaction # {}", t.Client_id, t.Transaction_type, t.Transaction_id) ;
        }
    } else {
        // If we got here, we know the account has a value
        let acct = opt_acct.unwrap() ;

        match t.Transaction_type.as_str() {
            "deposit" => {
                let available = acct.available + t.Amount.unwrap() ;
                let total = acct.total + t.Amount.unwrap() ;
                trx.execute("update client_account set available =?1, total= ?2 where client_id = ?3", params![available, total, t.Client_id]).expect("Failed to update account") ;
            },
            "withdrawal" => {
                let available = acct.available - t.Amount.unwrap() ;
                if available >= 0.0 {
                    let total = acct.total - t.Amount.unwrap() ;
                    trx.execute("update client_account set available =?1, total= ?2 where client_id =?3", params![available, total, t.Client_id]).expect("Error updating account") ;
                }
            },
            "dispute" => {
                let t_original = get_trade_transaction(&trx, t.Transaction_id) ;

                // Did we find the transaction we're disputing? If so, we proceed to handle the dispute
                if t_original.is_some() {
                    let t_orig = t_original.unwrap() ;

                    // Assumption: Client is only going to dispute withdrawals. The amount would have
                    // already been debited from the balance, so there's no need to subtract it again.
                    // We do need to update the held amount (and by extension the total)
                    if t_orig.Transaction_type == String::from("withdrawal") {
                        let held = acct.held + t_orig.Amount.unwrap();
                        let total = acct.total + t_orig.Amount.unwrap();
                        trx.execute("update client_account set held =?1, total = ?2 where client_id =?3;", params![held, total, t.Client_id]).expect("Error updating account") ;
                        trx.execute("insert into dispute values (?1, ?2, ?3, ?4);",params![t.Client_id, t.Transaction_id, t_orig.Amount,"disputed"]).expect("Error updating dispute" );
                    }
                  } else {
                    // This shouldn't happen very often, but it's not a fatal error, so we just ignore this transaction
                    eprintln!("Cannot find transaction {} to dispute", t.Transaction_id) ;
                }
            },
            "resolve" => {
                let t_original = get_trade_transaction(&trx, t.Transaction_id) ;

                // Again we can resolve this transaction only if there is a corresponding transaction to resolve
                if t_original.is_some() {
                    let t_orig = t_original.unwrap() ;

                    // Make sure there's an existing dispute to resolve
                    let dispute = get_dispute(&trx,t_orig.Client_id, t_orig.Transaction_id) ;
                    // Rule: if there is no pending dispute, then ignore this record
                    if dispute.is_none() {
                        return ;
                    }

                    // The issue has been resolved in favor of the trading firm, so we simply let
                    // the withdrawal amount stand and we reduce the hold amount as well as the total
                    if t_orig.Transaction_type == String::from("withdrawal") {

                        let held = acct.held - t_orig.Amount.unwrap();
                        let total = acct.total - t_orig.Amount.unwrap();

                        trx.execute("update client_account set held = ?1, total = ?2 where client_id =?3",
                                     params![held, total, t.Client_id]).expect("Error resolving dispute") ;

                        trx.execute("update dispute set status='resolved' where client_id = ?1 and event_id = ?2",
                                     params![t.Client_id, t.Transaction_id]).expect("Unable to update dispute") ;
                    }

                } else {
                    eprintln!("Cannot find transaction {} nor the dispute to resolve", t.Transaction_id) ;
                }

            },
            "chargeback" => {
                let t_original = get_trade_transaction(&trx, t.Transaction_id) ;

                if t_original.is_some() {
                    let t_orig = t_original.unwrap() ;

                    // Make sure there's an issue to resolve
                    let dispute = get_dispute(&trx, t_orig.Client_id, t_orig.Transaction_id) ;
                    // Rule: if there is no pending dispute, then ignore this record
                    if dispute.is_none() {
                        return ;
                    }

                    // The issue has been resolved in favor of the client and so we credit the
                    // customer and lock the account
                    if t_orig.Transaction_type.as_str() == "withdrawal" {

                        let held = acct.held - t_orig.Amount.unwrap();
                        let available = acct.available + t_orig.Amount.unwrap();

                        trx.execute("update client_account set held = ?1, available = ?2, locked = 1 where client_id =?3",
                                     params![held, available, t.Client_id]).expect("Error resolving dispute") ;

                        trx.execute("update dispute set status='chargedback' where client_id = ?1 and event_id = ?2",
                                     params![t.Client_id, t.Transaction_id]).expect("Unable to update dispute status") ;
                    }

                }
            },
            _ => panic!("Unknown transaction type .. (this should be impossible!)")
        }
   }

   // Add the transaction for deposits and withdrawals only - the other events are used to manage a dispute table
   if t.Transaction_type.as_str() == "deposit" || t.Transaction_type.as_str() == "withdrawal" {
       let res = trx.execute("insert into trade_event (event_id, client_id, event_type, amount) values (?1, ?2, ?3, ?4)",
                              params![t.Transaction_id, t.Client_id, t.Transaction_type, t.Amount]);

       if res.is_err() {
           eprintln!("Failed to record transaction # {}", t.Transaction_id);
       }
   }
   trx.commit().expect("Commit failed");

}

