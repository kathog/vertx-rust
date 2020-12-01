use hyper::{Client, Body, Uri, Response, Error, Request, Method};
use hyper::client::{HttpConnector, ResponseFuture};
use tokio::runtime::Runtime;


#[cfg(test)]
mod test {
    use crate::http::client::WebClient;
    extern crate pretty_env_logger;
    use log::{info, warn};
    use tokio::runtime::Runtime;

    #[test]
    fn test_blocking_get() {
        pretty_env_logger::init_timed();

        let client = WebClient::new();
        let response = client.blocking_get("http://127.0.0.1:9091");
        info!("{:?}", response);
    }

    #[test]
    fn test_get() {
        pretty_env_logger::init_timed();
        let rt = Runtime::new().unwrap();
        let client = WebClient::new();

        let response = rt.block_on(client.get("http://127.0.0.1:9092"));
        info!("{:?}", response);
    }

}

struct WebClient {
    client: Client<HttpConnector, Body>,
    // tls_client: Client<HttpsConnector<HttpConnector>, Body>,
    runtime: Runtime,
}

impl WebClient {
    pub fn new() -> Self {
        let runtime = Runtime::new().unwrap();
        // let https = HttpsConnector::new();
        // let tls_client = Client::builder().build::<_, Body>(https);
        WebClient {
            client: Client::new(),
            runtime,
            // tls_client
        }
    }

    #[inline]
    pub fn blocking_get(&self, url: &'static str) -> Result<Response<Body>, Error>{
        self.runtime.block_on(
            self.client.get(Uri::from_static(url))
        )
    }

    #[inline]
    pub fn blocking_post(&self, request: Request<Body>) -> Result<Response<Body>, Error> {
        self.runtime.block_on(
            self.client.request(request)
        )
    }

    #[inline]
    pub fn get(&self, url: &'static str) -> ResponseFuture {
        self.client.get(Uri::from_static(url))
    }

}