use std::convert::TryInto;
use std::sync::Arc;
use std::ops::Deref;

//Message used in event bus in standalone instance and cluster
#[derive(Clone, Default, Debug)]
pub struct Message {
    //Destination sub address
    pub(crate) address: Option<String>,
    //Replay sub address
    pub(crate) replay: Option<String>,
    //Binary body content
    pub(crate) body: Arc<Body>,
    //Protocol version
    pub(crate) protocol_version: i32,
    //System codec id
    pub(crate) system_codec_id: i32,
    //Port to replay message
    pub(crate) port: i32,
    //Host to replay message
    pub(crate) host: String,
    //Headers
    pub(crate) headers: i32,
    //Message send as publish to all nodes in sub
    pub(crate) publish: bool,
}

#[derive(Clone, Debug)]
pub enum Body {

    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    ByteArray(Vec<u8>),
    Boolean(bool),
    Null

}

impl Default for Body {
    fn default() -> Self {
        Self::Null
    }
}

impl Message {
    #[inline]
    pub fn body(&self) -> Arc<Body> {
        return self.body.clone();
    }

    //Reply message to event bus
    #[inline]
    pub fn reply(&mut self, data: Body) {
        self.body = Arc::new(data);
        self.address = self.replay.clone();
        self.replay = None;
    }

    pub fn generate() -> Message {
        Message {
            address: Some("test.01".to_string()),
            replay: Some(format!(
                "__vertx.reply.{}",
                uuid::Uuid::new_v4().to_string()
            )),
            body: Arc::new(Body::String(uuid::Uuid::new_v4().to_string())),
            port: 44532 as i32,
            host: "localhost".to_string(),
            ..Default::default()
        }
    }
}

//Implementation of deserialize byte array to message
impl From<Vec<u8>> for Message {
    #[inline]
    fn from(msg: Vec<u8>) -> Self {
        let mut idx = 1; //Ignore first 3 bytes
        let system_codec_id = i8::from_be_bytes(msg[idx..idx + 1].try_into().unwrap()) as i32;
        idx += 2;
        let len_addr = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let address = String::from_utf8(msg[idx..idx + len_addr].to_vec()).unwrap();
        idx += len_addr;
        let len_replay = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let mut replay = None;
        if len_replay > 0 {
            let replay_str = String::from_utf8(msg[idx..idx + len_replay].to_vec()).unwrap();
            idx += len_replay;
            replay = Some(replay_str);
        }
        let port = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let len_host = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let host = String::from_utf8(msg[idx..idx + len_host].to_vec()).unwrap();
        idx += len_host;
        let headers = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap());
        idx += 4;
        let body;
        match system_codec_id {
            0 => {
                body = Body::Null
            }
            3 => {
                body = Body::Boolean(i8::from_be_bytes(msg[idx..idx + 1].try_into().unwrap()) == 1)
            }
            5 => {
                body = Body::Int(i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()))
            },
            6 => {
                body = Body::Long(i64::from_be_bytes(msg[idx..idx + 8].try_into().unwrap()))
            },
            7 => {
                body = Body::Float(f32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()))
            },
            8 => {
                body = Body::Double(f64::from_be_bytes(msg[idx..idx + 8].try_into().unwrap()))
            },
            9 => {
                let len_body = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
                idx += 4;
                let body_array = msg[idx..idx + len_body].to_vec();
                body = Body::String(String::from_utf8(body_array).unwrap())
            },
            12 => {
                let len_body = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
                idx += 4;
                let body_array = msg[idx..idx + len_body].to_vec();
                body = Body::ByteArray(body_array)
            },
            _ => panic!("system_codec_id: {} not supported", system_codec_id)
        }

        Message {
            address: Some(address.to_string()),
            replay,
            port,
            host,
            headers,
            body: Arc::new(body),
            system_codec_id,
            ..Default::default()
        }
    }
}

impl Message {
    //Serialize message to byte array
    #[inline]
    pub fn to_vec(&self) -> Result<Vec<u8>, &str> {
        let mut data = vec![];
        data.push(1);

        let mut b0 = vec![];
        match self.body.deref() {
            Body::Int(b) => {
                data.push(5);
                b0.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Long(b) => {
                data.push(6);
                b0.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Float(b) => {
                data.push(7);
                b0.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::Double(b) => {
                data.push(8);
                b0.extend_from_slice(b.to_be_bytes().as_slice());
            }
            Body::String(b) => {
                data.push(9);
                b0.extend_from_slice(&(b.len() as i32).to_be_bytes());
                b0.extend_from_slice(b.as_bytes());
            }
            Body::ByteArray(b) => {
                data.push(12);
                b0.extend_from_slice(&(b.len() as i32).to_be_bytes());
                b0.extend_from_slice(b.as_slice());
            }
            Body::Boolean(b) => {
                data.push(3);
                if *b {
                    b0.extend_from_slice((1 as i8).to_be_bytes().as_slice())
                } else {
                    b0.extend_from_slice((0 as i8).to_be_bytes().as_slice())
                }
            }
            Body::Null => {}
        };


        data.push(0);
        let address = self.address.clone().expect("Replay message not found!");
        data.extend_from_slice(&(address.len() as i32).to_be_bytes());
        data.extend_from_slice(address.as_bytes());
        match self.replay.clone() {
            Some(addr) => {
                data.extend_from_slice(&(addr.len() as i32).to_be_bytes());
                data.extend_from_slice(addr.as_bytes());
            }
            None => {
                data.extend_from_slice(&(0 as i32).to_be_bytes());
            }
        }
        data.extend_from_slice(&self.port.to_be_bytes());
        data.extend_from_slice(&(self.host.len() as i32).to_be_bytes());
        data.extend_from_slice(self.host.as_bytes());
        data.extend_from_slice(&(4 as i32).to_be_bytes());
        // data.extend_from_slice(&(self.body.len() as i32).to_be_bytes());
        data.extend_from_slice(b0.as_slice());

        let len = ((data.len()) as i32).to_be_bytes();
        for idx in 0..4 {
            data.insert(idx, len[idx]);
        }
        return Ok(data);
    }
}
