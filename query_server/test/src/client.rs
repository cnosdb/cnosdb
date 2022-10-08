use reqwest::Url;
use reqwest::{Method, Request};

use crate::db_request::{DBRequest, Instruction, LineProtocol, Query};

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

    /// execute one sql at http://domain/query
    pub async fn execute_query(&self, query: &Query, buffer: &mut String) {
        buffer.push_str(format!("-- EXECUTE SQL: {} --\n", query.as_str()).as_str());

        let request_build = self.build_query_request(query);
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

    pub async fn execute_write(&self, line_protocol: &LineProtocol, buffer: &mut String) {
        buffer.push_str(
            format!(
                "-- WRITE LINE PROTOCOL --\n{}-- LINE PROTOCOL END --\n",
                line_protocol.as_str()
            )
            .as_str(),
        );
        let request_build = self.build_write_request(line_protocol);
        if request_build.is_err() {
            buffer.push_str("-- ERROR: request build fail --\n\n");
            return;
        }

        let request = request_build.unwrap();

        if let Ok(resp) = self.client.execute(request).await {
            let status_code = &resp.status();
            if status_code.is_success() {
                buffer.push_str(status_code.to_string().as_str());
                buffer.push_str("\n\n");
            } else {
                if let Ok(text) = resp.text().await {
                    buffer.push_str(text.as_str());
                    buffer.push('\n');
                }
                buffer.push_str("-- ERROR: --\n\n");
            }
        } else {
            buffer.push_str("-- ERROR: --\n\n");
        }
    }

    pub async fn execute_db_request(&self, db_requests: &Vec<DBRequest>) -> String {
        let mut buffer = String::new();
        for request in db_requests {
            match request {
                DBRequest::Query(query) => {
                    self.execute_query(query, &mut buffer).await;
                }
                DBRequest::LineProtocol(line_protocol) => {
                    self.execute_write(line_protocol, &mut buffer).await;
                }
            }
        }
        buffer
    }

    fn construct_query_url(&self, instruction: &Instruction) -> Url {
        let mut url = self.url.join("sql").unwrap();

        let mut http_query = String::new();
        http_query.push_str("db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        if instruction.pretty() {
            http_query.push_str("&pretty=true");
        }
        url.set_query(Some(http_query.as_str()));
        url
    }

    fn build_query_request(&self, query: &Query) -> Result<Request, reqwest::Error> {
        let url = self.construct_query_url(query.instruction());

        let mut body = String::new();
        body.push_str(query.as_str());

        self.client
            .request(Method::POST, url)
            .basic_auth::<&str, &str>(query.instruction().user_name(), None)
            .body(body)
            .build()
    }

    fn construct_write_url(&self, instruction: &Instruction) -> Url {
        let mut url = self.url.join("write").unwrap();

        let mut http_query = String::new();
        http_query.push_str("db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        if instruction.pretty() {
            http_query.push_str("&pretty=true");
        }
        url.set_query(Some(http_query.as_str()));
        url
    }

    fn build_write_request(&self, line_protocol: &LineProtocol) -> crate::error::Result<Request> {
        let url = self.construct_write_url(line_protocol.instruction());
        let mut body = String::new();
        body.push_str(line_protocol.as_str());

        Ok(self
            .client
            .request(Method::POST, url)
            .basic_auth::<&str, &str>(line_protocol.instruction().user_name(), None)
            .body(body)
            .build()?)
    }
}

#[tokio::test]
async fn test_ping() {
    use crate::CLIENT;
    if CLIENT.ping().await {
        println!("sucess");
    }
}
