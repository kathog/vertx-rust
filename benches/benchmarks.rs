#![feature(test)]
extern crate test;
#[macro_use]
extern crate lazy_static;
use crossbeam_channel::unbounded;
use vertx_rust::vertx::message::{Message, Body};
extern crate vertx_rust;
use std::sync::Arc;
use vertx_rust::vertx::cm::NoClusterManager;
use vertx_rust::vertx::*;
use test::Bencher;

lazy_static! {
    static ref VERTX: Vertx<NoClusterManager> = {
        let vertx_options = VertxOptions::default();
        let vertx = Vertx::<NoClusterManager>::new(vertx_options);
        vertx
    };
    static ref EB: Arc<EventBus<NoClusterManager>> = VERTX.event_bus();
}

#[bench]
fn vertx_request(b: &mut test::Bencher) {
    EB.local_consumer("test.01", move |m, _| {
        let b = m.body();
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = b.as_string().unwrap()
        );
        m.reply(Body::String(response));
    });

    b.iter(|| {
        let (tx, rx) = unbounded();
        EB.request("test.01", Body::String("UP".to_string()), move |m, _| {
            let _body = m.body();
            let _ = tx.send(1);
        });
        let _ = rx.recv();
    });
}

#[bench]
fn vertx_send(b: &mut test::Bencher) {
    EB.local_consumer("test.01", move |m, _| {
        let b = m.body();
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = b.as_string().unwrap()
        );
        m.reply(Body::String(response));
    });

    b.iter(|| {
        EB.send("test.01", Body::String("UP".to_string()));
    });
}

#[bench]
fn vertx_publish(b: &mut test::Bencher) {
    EB.local_consumer("test.01", move |m, _| {
        let b = m.body();
        let response = format!(
            r#"{{"health": "{code}"}}"#,
            code = b.as_string().unwrap()
        );
        m.reply(Body::String(response));
    });

    b.iter(|| {
        EB.publish("test.01", Body::String("UP".to_string()));
    });
}

#[bench]
fn serialize_message(b: &mut test::Bencher) {
    let m = Message::generate();

    b.iter(|| m.to_vec().unwrap());
}

#[bench]
fn deserialize_message(b: &mut test::Bencher) {
    let m = Message::generate();
    let bytes = m.to_vec().unwrap()[4..].to_vec();

    b.iter(|| {
        let _ = Message::from(bytes.clone());
    });
}

#[bench]
fn unbounded_crossbeam_uncontended(b: &mut Bencher) {
    let (tx, rx) = crossbeam_channel::unbounded();
    b.iter(|| {

            let m = Message::generate();
            let _ = tx.send(m);

            let _ = rx.recv();
    })
}
