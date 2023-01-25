#![feature(test)]
extern crate test;
#[macro_use]
extern crate lazy_static;
use crossbeam_channel::{bounded};
use vertx_rust::vertx::message::{Message, Body};
extern crate vertx_rust;
use std::sync::Arc;
use vertx_rust::vertx::cm::NoClusterManager;
use vertx_rust::vertx::*;
use criterion::{criterion_group, criterion_main, Criterion};
// use criterion_cycles_per_byte::CyclesPerByte;

lazy_static! {
    static ref RT : tokio::runtime::Runtime = rt();

    static ref VERTX : Vertx<NoClusterManager> = {
        let vertx_options = VertxOptions::default();
        RT.block_on(async {Vertx::new(vertx_options)})
    };

    static ref EVENT_BUS : Arc<EventBus<NoClusterManager>> = {
        let event_bus = RT.block_on(VERTX.event_bus());
        RT.block_on(async {
            event_bus.local_consumer("test.01", move |m, _| {
                Box::pin(async {
                    let b = m.body();
                    let response = format!(
                        r#"{{"health": "{code}"}}"#,
                        code = b.as_i32().unwrap()
                    );
                    m.reply(Body::String(response));
                })
            });
        });
        event_bus
    };
}


fn criterion_benchmark(c: &mut Criterion) {
    let m = Message::generate();
    let bytes = m.to_vec().unwrap()[4..].to_vec();
    c.bench_function("serialize_message", |b| b.iter(|| {
        m.to_vec().unwrap();
    }
    ));
    c.bench_function("deserialize_message", |b| b.iter(|| {
            let _ = Message::from(bytes.clone());
        }
    ));

}

// fn bench(c: &mut Criterion<CyclesPerByte>) {
//     let m = Message::generate();
//     let bytes = m.to_vec().unwrap()[4..].to_vec();
//     c.bench_function("serialize_cycles", |b| b.iter(|| {
//             m.to_vec().unwrap();
//         }
//     ));
//     c.bench_function("deserialize_cycles", |b| b.iter(|| {
//             let _ = Message::from(bytes.clone());
//         }
//     ));
// }


fn criterion_vertx(c: &mut Criterion) {

    c.bench_function("vertx_request", |b| b.iter(|| {
        let (tx, rx) = bounded(1);
        EVENT_BUS.request("test.01", Body::Int(102), move |m, _| {
            let _ = tx.send(m.body());
        });
        let _ = rx.recv().unwrap();
    }));

    c.bench_function("vertx_send", |b| b.iter(|| {
        EVENT_BUS.send("test.01", Body::Int(102));
    }));

    c.bench_function("vertx_publish", |b| b.iter(|| {
        EVENT_BUS.publish("test.01", Body::Int(102));
    }));
}


fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

criterion_group!(benches2, criterion_benchmark, criterion_vertx);

// criterion_group!(
//     name = benches;
//     config = Criterion::default().with_measurement(CyclesPerByte);
//     targets = bench
// );


criterion_main!( benches2);