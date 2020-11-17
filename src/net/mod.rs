use crate::vertx::RUNTIME;
use tokio::net::TcpListener;
use tokio::prelude::*;
use log::{error, info, debug};

#[cfg(test)]
mod tests {
    use crate::net;
    use log::{error, info, debug};
    use net::NetServer;
    use simple_logger::SimpleLogger;

    #[test]
    fn net_test() {
        SimpleLogger::new().init().unwrap();

        let mut net_server = NetServer::new();
        net_server.listen(9091, |req| {
            let mut resp = vec![];

            debug!("{:?}", String::from_utf8_lossy(req));
            let data = r#"
HTTP/1.1 200 OK
content-type: application/json
Date: Sun, 03 May 2020 07:05:15 GMT
Content-Length: 14

{"code": "UP"}
"#.to_string();

            resp.extend_from_slice(data.as_bytes());
            resp
        });

        std::thread::park();
    }

}


pub struct NetServer {

    pub port: u16,

}

impl NetServer {

    pub fn new() -> NetServer {
        NetServer {
            port: 0
        }
    }

    pub fn listen<OP>(&mut self, port: u16,  op: OP)
    where OP: Fn(&Vec<u8>) -> Vec<u8> + 'static + Send + Sync + Copy {
        let listener = RUNTIME.block_on(TcpListener::bind(format!("0.0.0.0:{}", port))).unwrap();
        self.port = listener.local_addr().unwrap().port();

        std::thread::spawn(move || {
        loop {
            let (mut socket, _) = RUNTIME.block_on(listener.accept()).unwrap();
            RUNTIME.spawn(async move {
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

                    let data = op(&request);

                    if let Err(e) = socket.write_all(&data).await {
                        eprintln!("failed to write to socket; err = {:?}", e);
                        return;
                    }
                }
            });
        }});
    }

}