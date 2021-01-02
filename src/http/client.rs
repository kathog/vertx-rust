use futures::TryStreamExt;
use hyper::client::{HttpConnector, ResponseFuture};
use hyper::{Body, Client, Error, Request, Response, Uri};
use tokio::runtime::Runtime;
use hyper_tls::HttpsConnector;

#[cfg(test)]
mod test {
    use crate::http::client::WebClient;
    extern crate pretty_env_logger;
    use hyper::{Body, Request};
    use log::info;
    use tokio::runtime::Runtime;

    #[test]
    fn test_blocking_get() {
        pretty_env_logger::init_timed();

        let client = WebClient::new();
        let response = client.blocking_get("http://127.0.0.1:9092");
        info!("{:?}", response);
    }

    #[test]
    fn test_blocking_request() {
        pretty_env_logger::init_timed();

        let client = WebClient::new();
        let body = Body::from("test_blocking_request");
        let request = Request::post("http://127.0.0.1:9092").body(body).unwrap();

        let response = client.blocking_request(request);
        let _ = response.map(|r| WebClient::blocking_body(r.into_body())
            .map(|b| {
                info!("{:?}", std::str::from_utf8(&*b));
                b
            })
        );
    }

    #[test]
    fn test_get() {
        pretty_env_logger::init_timed();
        let rt = Runtime::new().unwrap();
        let client = WebClient::new();

        let response = rt.block_on(client.get("https://github.com"));
        info!(
            "{:?}",
            WebClient::blocking_body(response.unwrap().into_body())
        );
    }
}

pub struct WebClient {
    client: Client<HttpConnector, Body>,
    tls_client: Client<HttpsConnector<HttpConnector>, Body>,
    runtime: Runtime,
}

#[allow(dead_code)]
impl WebClient {
    pub fn new() -> Self {
        let runtime = Runtime::new().unwrap();
        let https = HttpsConnector::new();
        let tls_client = Client::builder().build::<_, Body>(https);
        WebClient {
            client: Client::new(),
            runtime,
            tls_client
        }
    }

    #[inline]
    pub fn blocking_body(body: Body) -> Result<Vec<u8>, Error> {
        futures::executor::block_on(WebClient::get_body(body))
    }

    #[inline]
    pub async fn get_body(body: Body) -> Result<Vec<u8>, Error> {
        let body = body.try_fold(Vec::new(), |mut data, chunk| async move {
            data.extend_from_slice(&chunk);
            Ok(data)
        });
        body.await
    }

    #[inline]
    pub fn blocking_get(&self, url: &'static str) -> Result<Response<Body>, Error> {
        self.runtime.block_on(self.get(url))
    }

    #[inline]
    pub fn blocking_request(&self, request: Request<Body>) -> Result<Response<Body>, Error> {
        self.runtime.block_on(self.request(request))
    }

    #[inline]
    pub fn request(&self, request: Request<Body>) -> ResponseFuture {
        if request.uri().scheme_str() == Some("https") {
            self.tls_client.request(request)
        } else {
            self.client.request(request)
        }
    }

    #[inline]
    pub fn get(&self, url: &'static str) -> ResponseFuture {
        let uri = Uri::from_static(url);
        if uri.scheme_str() == Some("https") {
            self.tls_client.get(uri)
        } else {
            self.client.get(uri)
        }
    }
}
