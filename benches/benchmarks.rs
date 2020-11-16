use criterion::{criterion_group, criterion_main, Criterion};
#[macro_use]
extern crate lazy_static;

    
fn vertx(_c: &mut Criterion) {
    extern crate vertx_rust;
    use vertx_rust::vertx::*;
    use std::sync::Arc;

    lazy_static! {
        static ref VERTX : Vertx::<NoClusterManager> = {
            let vertx_options = VertxOptions::default();
            // println!("{:?}", vertx_options);
            Vertx::new(vertx_options)
        };
        static ref EVENT_BUS : Arc<EventBus> = VERTX.event_bus();
    }

    EVENT_BUS.consumer("consume1", |m| {
        let body = m.body();
        // println!("{:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
        m.reply(format!("response => {}", std::str::from_utf8(&body).unwrap()).as_bytes().to_vec());
    });

    _c.bench_function("vertx_request_callback", |b| b.iter(|| {
        EVENT_BUS.request_with_callback("consume1", "regest".to_owned(), move |m| {
            let _body = m.body();
           
        });
    }));

}


criterion_group!(benches, vertx);
criterion_main!(benches);