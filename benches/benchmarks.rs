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
    static ref RT : tokio::runtime::Runtime = rt();
    static ref VERTX: Vertx<NoClusterManager> = {
        std::env::set_var("RUST_LOG", "trace");
        let vertx_options = VertxOptions::default();
        let vertx = RT.block_on(async {Vertx::<NoClusterManager>::new(vertx_options)});
        vertx
    };
    static ref EB: Arc<EventBus<NoClusterManager>> = RT.block_on(VERTX.event_bus());
}

// #[bench]
// fn vertx_send(b: &mut test::Bencher) {
//     EB.local_consumer("test.01", move |m, _| {
//         let b = m.body();
//         let response = format!(
//             r#"{{"health": "{code}"}}"#,
//             code = b.as_string().unwrap()
//         );
//         m.reply(Body::String(response));
//     });
//
//     b.iter(|| {
//         EB.send("test.01", Body::String("UP".to_string()));
//     });
// }
//
// #[bench]
// fn vertx_publish(b: &mut test::Bencher) {
//     EB.local_consumer("test.01", move |m, _| {
//         let b = m.body();
//         let response = format!(
//             r#"{{"health": "{code}"}}"#,
//             code = b.as_string().unwrap()
//         );
//         m.reply(Body::String(response));
//     });
//
//     b.iter(|| {
//         EB.publish("test.01", Body::String("UP".to_string()));
//     });
// }

// fn vertx_benchmark(c: &mut Criterion) {
//     let tokio_rt = tokio::runtime::Builder::new_multi_thread()
//         .enable_time()
//         .enable_io()
//         .build()
//         .unwrap();
//
//         let vertx_options = VertxOptions::default();
//         let vertx = tokio_rt.block_on(async {Vertx::<NoClusterManager>::new(vertx_options)});
//         let event_bus = tokio_rt.block_on(vertx.event_bus());
//
//     tokio_rt.block_on( async {
//         event_bus.local_consumer("test.01", move |m, _| {
//             let b = m.body();
//             let response = format!(
//                 r#"{{"health": "{code}"}}"#,
//                 code = b.as_string().unwrap()
//             );
//             m.reply(Body::String(response));
//         });
//
//     });
//     let (tx, rx) = unbounded();
//     let tx0 = tx.clone();
//     c.bench_function("vertx_request", |b| b.iter(|| {
//         let tx1 = tx0.clone();
//         tokio_rt.block_on( async {
//             event_bus.request("test.01", Body::String("UP".to_string()), move |m, _| {
//                 let _body = m.body();
//                 let _ = tx1.send(1);
//             });
//         });
//         let _ = rx.recv();
//     }
//     ));
//
//
// }

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

// fn criterion_vertx(c: &mut Criterion) {
//
//     std::thread::spawn(move || {
//         let rt = rt();
//         rt.block_on(async move {
//             std::env::set_var("RUST_LOG", "trace");
//             let vertx_options = VertxOptions::default();
//             let vertx: Vertx<NoClusterManager> = Vertx::new(vertx_options);
//             let event_bus = vertx.event_bus().await;
//             event_bus.local_consumer("test.01", move |m, _| {
//                 let b = m.body();
//                 let response = format!(
//                     r#"{{"health": "{code}"}}"#,
//                     code = b.as_string().unwrap()
//                 );
//                 m.reply(Body::String(response));
//             });
//
//
//             let net_server = vertx.create_net_server();
//             net_server.listen(9091, move |_req, ev| {
//                 let mut resp = vec![];
//                 let (tx, rx) = bounded(1);
//                 ev.request(
//                     "test.01",
//                     Body::Int(101),
//                     move |m, _| {
//                         let _ = tx.send(m.body());
//                     },
//                 );
//
//                 let body = rx.recv().unwrap();
//                 let body = body.as_bytes().unwrap();
//                 let data = format!(
//                     r#"
// HTTP/1.1 200 OK
// content-type: application/json
// Date: Sun, 03 May 2020 07:05:15 GMT
// Content-Length: {len}
//
// {json_body}"#,
//                     len = body.len(),
//                     json_body = String::from_utf8_lossy(body)
//                 );
//                 resp.extend_from_slice(data.as_bytes());
//                 resp
//
//             }).await;
//             vertx.start().await;
//
//
//         });
//     });
//
//     std::thread::sleep(std::time::Duration::from_secs(10));
//
//     c.bench_function("vertx_request", |b| b.iter(|| {
//         let _ = reqwest::blocking::get("http://127.0.0.1:9091/").unwrap();
//     }));
// }


fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_io()
        .build()
        .unwrap()
}

criterion_group!(benches, criterion_benchmark, criterion_vertx);
criterion_main!(benches);