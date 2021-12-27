#![feature(test)]
extern crate test;
#[macro_use]
extern crate lazy_static;
use crossbeam_channel::{bounded, unbounded};
use vertx_rust::vertx::message::{Message, Body};
extern crate vertx_rust;
use std::sync::Arc;
use vertx_rust::vertx::cm::NoClusterManager;
use vertx_rust::vertx::*;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

lazy_static! {
    static ref MSG : Message = Message::generate();
    static ref BYTES : Vec<u8> = {
        MSG.to_vec().unwrap()[4..].to_vec()
    };
}


fn iai_serializable()  {
    MSG.to_vec().unwrap();
}

fn iai_deserializable()  {
    let _ = Message::from(BYTES.clone());
}

use iai::{main};
iai::main!(iai_serializable, iai_deserializable);
