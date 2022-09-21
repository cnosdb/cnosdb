use crate::query::Query;
use lazy_static::lazy_static;
use reqwest::Method;
use reqwest::Url;

pub struct Client {
    url: Url,
    client: reqwest::Client,
}

lazy_static! {
    pub static ref CLIENT: Client = Client::new("http://127.0.0.1:31007");
}

impl Client {
    pub fn new(url: &str) -> Client {
        let url = Url::parse(url).unwrap();
        Client {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub fn construct_query_url(&self, query: &Query) -> Url {
        let mut url = self.url.join("query").unwrap();

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
    /// execute one sql at http://domain/query
    pub async fn execute_query(&self, query: &Query, buffer: &mut String) {
        buffer.push_str(format!("-- EXECUTE SQL: {} --\n", query.as_str()).as_str());

        let url = self.construct_query_url(query);

        let mut body = String::new();
        body.push_str(query.as_str());

        let request = self
            .client
            .request(Method::POST, url)
            .body(body)
            .build()
            .unwrap();

        if let Ok(resp) = self.client.execute(request).await {
            let text = resp.text().await.unwrap();
            buffer.push_str(text.as_str());
            buffer.push_str("\n\n");
        } else {
            buffer.push_str(format!("-- ERROR: --\n\n").as_str());
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
        let url = self.url.join("ping").unwrap();
        println!("{}", &url);
        if let Ok(req) = reqwest::get(url).await {
            req.status().is_success()
        } else {
            false
        }
    }
}

#[tokio::test]
async fn test_ping() {
    let client = Client::new("http://127.0.0.1:31007");

    if client.ping().await {
        println!("sucess");
    }
}

#[tokio::test]
async fn test_query() {
    let client = Client::new("http://127.0.0.1:31007");

    let sqls = r##"
        Select 1;
    "##;
    let mut queries = Vec::new();
    Query::parse_queries(sqls, &mut queries);

    // client.execute_query(&queries[0], io::stdout()).await;
}
