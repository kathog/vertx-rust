#[macro_use]
extern crate jvm_macro;
extern crate jvm_serializable;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
use log::{debug, info};

#[cfg(test)]
mod tests {

    extern crate vertx_rust;
    use vertx_rust::vertx::*;
    use std::sync::Arc;
    use serde::{Serialize, Deserialize};
    use jvm_serializable::java::io::*;
    use tokio::net::TcpListener;
    use tokio::prelude::*;


    #[test]
    fn tcp_test () {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = runtime.block_on(TcpListener::bind("0.0.0.0:9091")).unwrap();

        loop {
            let (mut socket, _) = runtime.block_on(listener.accept()).unwrap();

            runtime.spawn(async move {

                let mut request: Vec<u8> = vec![];
                let mut buf = [0; 1024];

                loop {
                    let _n = match socket.read(&mut buf).await {
                        Ok(n) if n == 0 => return,
                        Ok(n) => request.extend(&buf[0..n]),
                        Err(e) => {
                            eprintln!("failed to read from socket; err = {:?}", e);
                            return;
                        }
                    };

                    // println!("{:?}", std::str::from_utf8(&request));

                    let data = r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 14

{"code": "UP"}
"#.to_string();
                    if let Err(e) = socket.write_all(data.as_bytes()).await {
                        eprintln!("failed to write to socket; err = {:?}", e);
                        return;
                    }
                }
            });
        }
    }

    #[jvm_object(io.vertx.core.net.impl.ServerID,5636540499169644934)]
    struct ServerID {
        port: i32,
        host: String
    }


    #[jvm_object(io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo,1)]
    struct ClusterNodeInfo {
        nodeId: String,
        serverID: ServerID,
    }

    #[jvm_object(java.lang.Object,0)]
    struct Object {
        key: String,
        value: String,
    }

    #[jvm_object(io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap$KeyValue,6529685098267757690)]
    struct ZKSyncMapKeyValue {
        key: Object,
    }

    #[test]
    // #[ignore = "tymczasowo"]
    fn vertx_test() {

        lazy_static! {
            static ref VERTX : Vertx::<NoClusterManager> = {
                let vertx_options = VertxOptions::default();
                debug!("{:?}", vertx_options);
                Vertx::new(vertx_options)
            };
            static ref EVENT_BUS : Arc<EventBus> = VERTX.event_bus();

            static ref COUNT : std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        }

        EVENT_BUS.consumer("consume1", |m| {
            let body = m.body();
            // println!("{:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
            m.reply(format!("response => {}", std::str::from_utf8(&body).unwrap()).as_bytes().to_vec());
        });

        let time = std::time::Instant::now();
        for i in 0..100000 {
            // event_bus.request("consume1", format!("regest: {}", i));
            // count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            EVENT_BUS.request_with_callback("consume1", format!("regest: {}", i), move |m| {
                let _body = m.body();
                COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // println!("set_callback_function {:?}, thread: {:?}", std::str::from_utf8(&body), std::thread::current().id());
            });
        }

        // vertx.start();
        // std::thread::sleep(std::time::Duration::from_millis(1));
        let elapsed = time.elapsed();
        info!("count {:?}, time: {:?}", COUNT.load(std::sync::atomic::Ordering::SeqCst), &elapsed);
        info!("avg time: {:?} ns", (&elapsed.as_nanos() / COUNT.load(std::sync::atomic::Ordering::SeqCst) as u128));
    }
}