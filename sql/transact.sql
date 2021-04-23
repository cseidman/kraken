drop table if exists client_account ;
create table client_account
(
    client_id integer primary key,
    available float,
    total float,
    held float,
    locked boolean
);
create index ix_account_client_id on client_account(client_id) ;

drop table if exists dispute ;
create table dispute (
    client_id integer,
    event_id integer,
    amount float, -- disputed amount
    status text -- 'resolved', 'disputed', 'chargedback'
);
create index ix_dispute on dispute(client_id, event_id) ;

drop table if exists trade_event ;
create table trade_event
(
    event_id integer primary key ,
    client_id integer,
    event_type text,
    amount float null
);
create index ix_client_id on trade_event(client_id) ;
create index ix_event_type_id on trade_event(event_type) ;






