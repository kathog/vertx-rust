use criterion::{criterion_group, criterion_main, Criterion};
#[macro_use]
extern crate lazy_static;

    
fn vertx(_c: &mut Criterion) {
    #[path = "../src/lib.rs"]
    extern crate vertx_rust;
    use vertx_rust::io::vertx::*;
    use std::sync::Arc;

    lazy_static! {
        static ref vertx : Vertx = {
            let vertx_options = VertxOptions::default();
            // println!("{:?}", vertx_options);
            Vertx::new(vertx_options)
        };
        static ref event_bus : Arc<EventBus> = vertx.event_bus();

        static ref count : std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0); 
    }

    event_bus.consumer("consume1", |m| {
        let body = m.body();
        // println!("{:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
        m.reply(format!("response => {}", std::str::from_utf8(&body).unwrap()).as_bytes().to_vec());
    });

    _c.bench_function("vertx_request_callback", |b| b.iter(|| {
        event_bus.request_with_callback("consume1", "regest".to_owned(), move |m| {
            let _body = m.body();
           
        });
    }));

}


criterion_group!(benches, vertx);
criterion_main!(benches);