
use reqwest::{Method, Request};
use reqwest::{Response, Url};
use std::time::Duration;
use tokio::time::timeout;

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
        if let Some(sleep) = query.instruction().sleep() {
            if sleep != 0 {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
            }
        }
        if query.instruction().sort() && query.is_return_result_set() {
            buffer.push_str("-- AFTER_SORT --\n")
        }
        let request_build = self.build_query_request(query);
        if request_build.is_err() {
            push_error(buffer, "request build fail");
            return;
        }
        let request = request_build.unwrap();

        match self.client.execute(request).await {
            Ok(resp) => {
                let status_code = resp.status();
                buffer.push_str(status_code.to_string().as_str());
                buffer.push('\n');
                push_query_result(buffer, query, resp).await;
                if !status_code.is_success() {
                    push_error(buffer, "");
                } else {
                    buffer.push('\n');
                }
            }
            Err(e) => {
                push_error(
                    buffer,
                    format!("send error ({:?}) for query ({})", e, query.as_str()).as_str(),
                );
            }
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
            push_error(buffer, "request build fail");
            return;
        }

        let request = request_build.unwrap();

        if let Ok(resp) = self.client.execute(request).await {
            let status_code = resp.status();
            buffer.push_str(status_code.to_string().as_str());
            buffer.push('\n');
            if let Ok(text) = resp.text().await {
                if !text.is_empty() {
                    buffer.push_str(text.as_str());
                    buffer.push('\n');
                }
            }
            if !status_code.is_success() {
                push_error(buffer, "");
            } else {
                buffer.push('\n');
            }
        } else {
            push_error(buffer, "");
        }
    }

    pub async fn execute_db_request(
        &self,
        case_name: &str,
        db_requests: &Vec<DBRequest>,
    ) -> String {
        let mut buffer = String::new();
        for (i, request) in db_requests.iter().enumerate() {
            if (i + 1) % 100 == 0 {
                println!("\t{}: {}/{}", case_name, i + 1, db_requests.len());
            }
            match request {
                DBRequest::Query(query) => {
                    if let Some(time) = query.instruction().time_out() {
                        if (timeout(
                            Duration::from_secs(time),
                            self.execute_query(query, &mut buffer),
                        )
                        .await)
                            .is_err()
                        {
                            push_error(&mut buffer, "TIMEOUT");
                        }
                    } else {
                        self.execute_query(query, &mut buffer).await;
                    }
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
        http_query.push_str("target_partitions=8&db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        http_query.push_str("&tenant=");
        http_query.push_str(instruction.tenant_name());

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

        http_query.push_str("&tenant=");
        http_query.push_str(instruction.tenant_name());

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

fn push_error(s: &mut String, error_message: &str) {
    s.push_str(format!("-- ERROR: {} --\n\n", error_message).as_str());
}

async fn push_query_result(buffer: &mut String, query: &Query, resp: Response) {
    let success = resp.status().is_success();
    if let Ok(text) = resp.text().await {
        if success && query.is_return_result_set() && query.instruction().sort() && !text.is_empty()
        {
            let mut lines: Vec<&str> = text.lines().collect();
            buffer.push_str(lines[0]);
            buffer.push('\n');

            let len = lines.len();
            let result_lines = &mut lines[1..len];
            result_lines.sort();
            buffer.push_str(result_lines.join("\n").as_str());
        } else {
            buffer.push_str(text.as_str());
        }
        buffer.push('\n');
    }
}

#[tokio::test]
async fn test_ping() {
    use crate::CLIENT;
    if CLIENT.ping().await {
        println!("sucess");
    }
}
