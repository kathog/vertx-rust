use criterion::{criterion_group, criterion_main, Criterion};
use crossbeam_channel::unbounded;


fn vertx(_c: &mut Criterion) {
    extern crate vertx_rust;
    use vertx_rust::vertx::*;

    let vertx_options = VertxOptions::default();
    let vertx =  Vertx::<NoClusterManager>::new(vertx_options);
    let event_bus = vertx.event_bus();

    event_bus.local_consumer("test.01", move |m, _| {
        let body = m.body();
        let response = format!(r#"{{"health": "{code}"}}"#, code=std::str::from_utf8(&body.to_vec()).unwrap());
        m.reply(response.into_bytes());
    });

    _c.bench_function("vertx_request", |b| b.iter(|| {
        let (tx,rx) = unbounded();
        event_bus.request("test.01", b"UP".to_vec(), move |m, _| {
            let _body = m.body();
            let _ = tx.send(1);
        });
        let _ = rx.recv();
    }));

    _c.bench_function("vertx_send", |b| b.iter(|| {
        event_bus.send("test.01", b"UP".to_vec());
    }));

    _c.bench_function("vertx_publish", |b| b.iter(|| {
        event_bus.send("test.01", b"UP".to_vec());
    }));
}


criterion_group!(benches, vertx);
criterion_main!(benches);