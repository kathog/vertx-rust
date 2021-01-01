use std::convert::TryInto;
use std::sync::Arc;

//Message used in event bus in standalone instance and cluster
#[derive(Clone, Default, Debug)]
pub struct Message {
    //Destination sub address
    pub(crate) address: Option<String>,
    //Replay sub address
    pub(crate) replay: Option<String>,
    //Binary body content
    pub(crate) body: Arc<Vec<u8>>,
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

impl Message {
    #[inline]
    pub fn body(&self) -> Arc<Vec<u8>> {
        return self.body.clone();
    }

    //Reply message to event bus
    #[inline]
    pub fn reply(&mut self, mut data: Vec<u8>) {
        unsafe {
            let mut clone_body = self.body.clone();
            let inner_body = Arc::get_mut_unchecked(&mut clone_body);
            inner_body.clear();
            inner_body.append(&mut data);
            self.address = self.replay.clone();
            self.replay = None;
        }
    }

    pub fn generate() -> Message {
        Message {
            address: Some("test.01".to_string()),
            replay: Some(format!(
                "__vertx.reply.{}",
                uuid::Uuid::new_v4().to_string()
            )),
            body: Arc::new(uuid::Uuid::new_v4().to_string().into_bytes()),
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
        let mut idx = 3; //Ignore first 3 bytes
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
        let len_body = i32::from_be_bytes(msg[idx..idx + 4].try_into().unwrap()) as usize;
        idx += 4;
        let body = msg[idx..idx + len_body].to_vec();
        Message {
            address: Some(address.to_string()),
            replay,
            port,
            host,
            headers,
            body: Arc::new(body),
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
        data.push(12);
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
        data.extend_from_slice(&(self.body.len() as i32).to_be_bytes());
        data.extend_from_slice(self.body.as_slice());
        let len = ((data.len()) as i32).to_be_bytes();
        for idx in 0..4 {
            data.insert(idx, len[idx]);
        }
        return Ok(data);
    }
}
