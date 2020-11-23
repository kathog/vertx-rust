use criterion::{criterion_group, criterion_main, Criterion};


fn vertx(_c: &mut Criterion) {
    extern crate vertx_rust;
    use vertx_rust::vertx::*;

    let vertx_options = VertxOptions::default();
    let vertx =  Vertx::<NoClusterManager>::new(vertx_options);
    let event_bus = vertx.event_bus();

    event_bus.consumer("test.01", move |m, _| {
        let body = m.body();
        let response = format!("{{\"health\": \"{code}\"}}", code=std::str::from_utf8(&body.to_vec()).unwrap());
        m.reply(response.into_bytes());
    });

    _c.bench_function("vertx_request_callback", |b| b.iter(|| {
        event_bus.request_with_callback("test.01", "UP".to_owned(), move |m, _| {
            let _body = m.body();
        });
    }));

    _c.bench_function("vertx_request", |b| b.iter(|| {
        event_bus.request("test.01", "UP".to_owned());
    }));
}


criterion_group!(benches, vertx);
criterion_main!(benches);