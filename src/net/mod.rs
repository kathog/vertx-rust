use crate::vertx::{RUNTIME, Message, EventBus, ClusterManager};
use tokio::net::{TcpListener};
use tokio::prelude::*;
use bytes::BytesMut;
use crossbeam_channel::Sender;
use std::sync::Arc;
use log::{info, error};

pub struct NetServer<CM:'static + ClusterManager + Send + Sync> {

    pub port: u16,
    event_bus: Option<Arc<EventBus<CM>>>

}

impl <CM:'static + ClusterManager + Send + Sync>NetServer<CM> {

    pub fn new(event_bus: Option<Arc<EventBus<CM>>>) -> &'static mut NetServer<CM> {
        Box::leak(Box::new(NetServer::<CM> {
            port: 0,
            event_bus
        }))
    }

    pub(crate) fn listen_for_message<OP>(&mut self, port: u16, op: OP)
    where OP: Fn(Vec<u8>, Sender<Message>) -> Vec<u8> + 'static + Send + Sync + Copy {
        let listener = RUNTIME.block_on(TcpListener::bind(format!("0.0.0.0:{}", port))).unwrap();
        self.port = listener.local_addr().unwrap().port();

        let ev = self.event_bus.clone();
        let ev = ev.unwrap().clone();
        let sender = ev.sender.lock().unwrap();

        let clonse_sender = sender.clone();
        std::thread::spawn(move || {
            loop {
                let inner_sender = clonse_sender.clone();
                let (mut socket, _) = RUNTIME.block_on(listener.accept()).unwrap();
                RUNTIME.spawn(async move {               
                    loop {
                        let inner_sender = inner_sender.clone();
                        let mut size = [0; 4];
                        #[allow(unused_assignments)]
                        let mut len = 0;
                        let _n = match socket.read(&mut size).await {
                            Ok(n) if n == 0 => return,
                            Ok(_n) => {
                                len = i32::from_be_bytes(size);
                            },
                            Err(e) => {
                                error!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };
                        let mut buf = BytesMut::with_capacity(len as usize);
                        let _n = match socket.read_buf(&mut buf).await {
                            Ok(n) if n == 0 => return,
                            Ok(n) => &buf[0..n],
                            Err(e) => {
                                error!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };
                        let bytes_as_vec = buf.to_vec();
                        let bytes_as_string = String::from_utf8_lossy(&bytes_as_vec);
                        if bytes_as_string.contains("ping") {
                            if let Err(e) = socket.write_all(b"pong").await {
                                error!("failed to write to socket; err = {:?}", e);
                                return;
                            }
                        } else {
                            let data = op(bytes_as_vec, inner_sender);
                            if let Err(e) = socket.write_all(&data).await {
                                error!("failed to write to socket; err = {:?}", e);
                                return;
                            }
                        }
                    }
                });
            }}
        );
    }



    pub fn listen<OP>(&'static mut self, port: u16,  op: OP)
    where OP: Fn(&Vec<u8>, Arc<EventBus<CM>>) -> Vec<u8> + 'static + Send + Sync + Copy {
        let listener = RUNTIME.block_on(TcpListener::bind(format!("0.0.0.0:{}", port))).unwrap();
        self.port = listener.local_addr().unwrap().port();
        info!("start net_server listen on port: {}", self.port);
        std::thread::spawn(move || {
            loop {
                let (mut socket, _) = RUNTIME.block_on(listener.accept()).unwrap();
                let ev = self.event_bus.as_ref().unwrap().clone();
                RUNTIME.spawn(async move {
                    loop {
                        let local_ev = ev.clone();
                        let mut request: Vec<u8> = vec![];                       
                        let mut buf = [0; 2048];
                        let _n = match socket.read(&mut buf).await {
                            Ok(n) if n == 0 => return,
                            Ok(n) => request.extend(&buf[0..n]),
                            Err(e) => {
                                error!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };
                        let data = op(&request, local_ev);
                        if let Err(e) = socket.write_all(&data).await {
                            error!("failed to write to socket; err = {:?}", e);
                            return;
                        }
                    }
                });
            }
        });
    }

}