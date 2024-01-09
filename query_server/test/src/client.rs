use std::time::Duration;

use reqwest::{Method, Request, Response, Url};
use tokio::time::timeout;

use crate::db_request::{
    DBRequest, Instruction, LineProtocol, OpenTSDBJson, OpenTSDBProtocol, Query, ShellScript,
};

pub struct Client {
    query_url: Url,
    storage_url: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn from_url(query_url: Url, storage_url: Option<Url>) -> Self {
        match storage_url {
            Some(u) => Self {
                query_url,
                storage_url: u,
                client: reqwest::Client::default(),
            },
            None => Self {
                query_url: query_url.clone(),
                storage_url: query_url,
                client: reqwest::Client::default(),
            },
        }
    }

    /// ping db succeed return true
    pub async fn ping(&self) -> bool {
        async fn ping_url(url: &Url) -> bool {
            let url = match url.join("api/v1/ping") {
                Ok(u) => u,
                Err(_) => return false,
            };
            if let Ok(req) = reqwest::get(url).await {
                req.status().is_success()
            } else {
                false
            }
        }

        ping_url(&self.query_url).await && ping_url(&self.storage_url).await
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
                if query.is_stream_respones() {
                    if let Err(err) = push_query_stream_result(buffer, query, resp).await {
                        push_error(buffer, &err.to_string());
                    }
                } else {
                    push_query_result(buffer, query, resp).await;
                }

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
        if let Some(sleep) = line_protocol.instruction().sleep() {
            if sleep != 0 {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
            }
        }
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

    pub async fn execute_opentsdb_write(
        &self,
        open_tsdb_protocol: &OpenTSDBProtocol,
        buffer: &mut String,
    ) {
        buffer.push_str(
            format!(
                "-- WRITE OPEN TSDB PROTOCOL --\n{}-- OPEN TSDB PROTOCOL END --\n",
                open_tsdb_protocol.as_str()
            )
            .as_str(),
        );
        if let Some(sleep) = open_tsdb_protocol.instruction().sleep() {
            if sleep != 0 {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
            }
        }
        let request_build = self.build_open_tsdb_write_request(open_tsdb_protocol);
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

    pub async fn execute_opentsdb_json_write(
        &self,
        open_tsdb_json: &OpenTSDBJson,
        buffer: &mut String,
    ) {
        buffer.push_str(
            format!(
                "-- WRITE OPEN TSDB JSON --\n{}-- OPEN TSDB JSON END --\n",
                open_tsdb_json.as_str()
            )
            .as_str(),
        );
        if let Some(sleep) = open_tsdb_json.instruction().sleep() {
            if sleep != 0 {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
            }
        }
        let request_build = self.build_open_tsdb_json_request(open_tsdb_json);
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

    pub async fn execute_shell_script(&self, shell_script: &ShellScript, _: &mut String) {
        println!("-- Start executing shell script\n");
        if let Some(sleep) = shell_script.instruction().sleep() {
            if sleep != 0 {
                tokio::time::sleep(Duration::from_millis(sleep)).await;
            }
        }
        let (code, output, error) =
            run_script::run_script!(shell_script.as_str()).expect("run shell script failed");
        println!("Exit Code: {}", code);
        println!("Output: {}", output);
        println!("Error: {}", error);
    }

    pub async fn execute_db_request(&self, case_name: &str, db_requests: &[DBRequest]) -> String {
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
                DBRequest::OpenTSDBProtocol(open_tsdb_protocol) => {
                    self.execute_opentsdb_write(open_tsdb_protocol, &mut buffer)
                        .await;
                }
                DBRequest::OpenTSDBJson(open_tsdb_json) => {
                    self.execute_opentsdb_json_write(open_tsdb_json, &mut buffer)
                        .await;
                }
                DBRequest::ShellScript(shell_script) => {
                    self.execute_shell_script(shell_script, &mut buffer).await;
                }
            }
        }
        buffer
    }

    fn construct_query_url(&self, instruction: &Instruction) -> Url {
        let mut url = self.query_url.join("api/v1/sql").unwrap();

        let mut http_query = String::new();
        http_query.push_str("target_partitions=8&db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        http_query.push_str("&tenant=");
        http_query.push_str(instruction.tenant_name());

        http_query.push_str("&precision=");
        http_query.push_str(instruction.precision().unwrap_or("NS"));

        http_query.push_str("&chunked=");
        http_query.push_str(instruction.chunked());

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
        let mut url = self.storage_url.join("api/v1/write").unwrap();

        let mut http_query = String::new();
        http_query.push_str("db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        http_query.push_str("&tenant=");
        http_query.push_str(instruction.tenant_name());

        http_query.push_str("&precision=");
        http_query.push_str(instruction.precision().unwrap_or("NS"));

        if instruction.pretty() {
            http_query.push_str("&pretty=true");
        }
        url.set_query(Some(http_query.as_str()));
        url
    }

    fn construct_opentsdb_write_url(&self, instruction: &Instruction) -> Url {
        let mut url = self.storage_url.join("api/v1/opentsdb/write").unwrap();

        let mut http_query = String::new();
        http_query.push_str("db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        http_query.push_str("&tenant=");
        http_query.push_str(instruction.tenant_name());

        http_query.push_str("&precision=");
        http_query.push_str(instruction.precision().unwrap_or("NS"));

        if instruction.pretty() {
            http_query.push_str("&pretty=true");
        }
        url.set_query(Some(http_query.as_str()));
        url
    }

    fn construct_opentsdb_json_url(&self, instruction: &Instruction) -> Url {
        let mut url = self.storage_url.join("api/v1/opentsdb/put").unwrap();

        let mut http_query = String::new();
        http_query.push_str("db=");

        if instruction.db_name().is_empty() {
            http_query.push_str("public");
        } else {
            http_query.push_str(instruction.db_name());
        }

        http_query.push_str("&tenant=");
        http_query.push_str(instruction.tenant_name());

        http_query.push_str("&precision=");
        http_query.push_str(instruction.precision().unwrap_or("NS"));

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

    fn build_open_tsdb_write_request(
        &self,
        open_tsdb_protocol: &OpenTSDBProtocol,
    ) -> crate::error::Result<Request> {
        let url = self.construct_opentsdb_write_url(open_tsdb_protocol.instruction());
        let mut body = String::new();
        body.push_str(open_tsdb_protocol.as_str());

        Ok(self
            .client
            .request(Method::POST, url)
            .basic_auth::<&str, &str>(open_tsdb_protocol.instruction().user_name(), None)
            .body(body)
            .build()?)
    }

    fn build_open_tsdb_json_request(
        &self,
        open_tsdb_protocol: &OpenTSDBJson,
    ) -> crate::error::Result<Request> {
        let url = self.construct_opentsdb_json_url(open_tsdb_protocol.instruction());
        let mut body = String::new();
        body.push_str(open_tsdb_protocol.as_str());

        Ok(self
            .client
            .request(Method::POST, url)
            .basic_auth::<&str, &str>(open_tsdb_protocol.instruction().user_name(), None)
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

async fn push_query_stream_result(
    buffer: &mut String,
    query: &Query,
    mut resp: Response,
) -> Result<(), Box<dyn std::error::Error>> {
    let success = resp.status().is_success();
    let mut str = String::with_capacity(1024 * 1024);
    while let Some(text) = resp.chunk().await? {
        let text = String::from_utf8(text.to_vec())?;
        str.push_str(&text);
    }
    if success && query.is_return_result_set() && query.instruction().sort() && !str.is_empty() {
        let mut lines: Vec<&str> = str.lines().collect();
        buffer.push_str(lines[0]);
        buffer.push('\n');

        let len = lines.len();
        let result_lines = &mut lines[1..len];
        result_lines.sort();
        buffer.push_str(result_lines.join("\n").as_str());
    } else {
        buffer.push_str(str.as_str());
    }
    buffer.push('\n');
    Ok(())
}

#[tokio::test]
async fn test_ping() {
    use crate::CLIENT;
    if CLIENT.ping().await {
        println!("sucess");
    }
}
