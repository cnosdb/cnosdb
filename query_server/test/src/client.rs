use reqwest::Url;
use reqwest::{Method, Request};

use crate::query::Query;

pub struct Client {
    url: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn from_url(url: Url) -> Client {
        Client {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub fn construct_query_url(&self, query: &Query) -> Url {
        let mut url = self.url.join("sql").unwrap();

        let mut http_query = String::new();
        http_query.push_str("db=");

        if query.instruction().db_name().is_empty() {
            http_query.push_str("test");
        } else {
            http_query.push_str(query.instruction().db_name());
        }

        if query.instruction().pretty() {
            http_query.push_str("&pretty=true");
        }
        url.set_query(Some(http_query.as_str()));
        url
    }

    fn build_request(&self, query: &Query) -> std::result::Result<Request, reqwest::Error> {
        let url = self.construct_query_url(query);

        let mut body = String::new();
        body.push_str(query.as_str());

        self.client
            .request(Method::POST, url)
            .basic_auth::<&str, &str>(query.instruction().user_name(), None)
            .body(body)
            .build()
    }

    /// execute one sql at http://domain/query
    pub async fn execute_query(&self, query: &Query, buffer: &mut String) {
        buffer.push_str(format!("-- EXECUTE SQL: {} --\n", query.as_str()).as_str());

        let request_build = self.build_request(query);
        if request_build.is_err() {
            buffer.push_str("-- ERROR: request build fail --\n\n");
            return;
        }
        let request = request_build.unwrap();

        if let Ok(resp) = self.client.execute(request).await {
            let status_code = &resp.status();

            if let Ok(text) = resp.text().await {
                buffer.push_str(status_code.to_string().as_str());
                buffer.push('\n');
                buffer.push_str(text.as_str());
                buffer.push_str("\n\n");
            } else {
                buffer.push_str("-- ERROR: --\n\n");
            }
        } else {
            buffer.push_str("-- ERROR: --\n\n");
        }
    }

    /// execute one sql and return text of response
    pub async fn execute_queries(&self, queries: &Vec<Query>) -> String {
        let mut buffer = String::new();

        for query in queries {
            self.execute_query(query, &mut buffer).await;
        }

        buffer
    }

    ///TODO(add line protocol)
    #[allow(dead_code)]
    pub async fn execute_write(&self) -> String {
        String::new()
    }

    /// ping db succeed return true
    pub async fn ping(&self) -> bool {
        let url = self.url.join("ping");
        if url.is_err() {
            return false;
        }
        let url = url.unwrap();
        if let Ok(req) = reqwest::get(url).await {
            req.status().is_success()
        } else {
            false
        }
    }
}

#[tokio::test]
async fn test_ping() {
    use crate::CLIENT;
    if CLIENT.ping().await {
        println!("sucess");
    }
}

#[tokio::test]
async fn test_query() {
    use crate::CLIENT;
    let sqls = r##"
        Select 1;
    "##;
    let queries = Query::parse_queries(sqls).unwrap();
    let res = CLIENT.execute_queries(&queries).await;

    println!("{:#?}", res);
}
