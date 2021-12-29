#![feature(test)]
extern crate test;
#[macro_use]
extern crate lazy_static;
use vertx_rust::vertx::message::{Message};
extern crate vertx_rust;

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

iai::main!(iai_serializable, iai_deserializable);
