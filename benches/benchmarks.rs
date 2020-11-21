use criterion::{criterion_group, criterion_main, Criterion};

    
fn vertx(_c: &mut Criterion) {
    extern crate vertx_rust;
    use vertx_rust::vertx::*;

    let vertx_options = VertxOptions::default();
    let vertx =  Vertx::<NoClusterManager>::new(vertx_options);
    let event_bus = vertx.event_bus();

    event_bus.consumer("consume1", |m, _| {
        let body = m.body();
        // println!("{:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
        m.reply(format!("response => {}", std::str::from_utf8(&body).unwrap()).as_bytes().to_vec());
    });

    _c.bench_function("vertx_request_callback", |b| b.iter(|| {
        event_bus.request_with_callback("consume1", "regest".to_owned(), move |m, _| {
            let _body = m.body();
        });
    }));

}


criterion_group!(benches, vertx);
criterion_main!(benches);