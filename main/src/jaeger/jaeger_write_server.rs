use chrono::Utc;
use coordinator::service::CoordinatorRef;
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::auth::user::{UserInfo, ROOT, ROOT_PWD};
use models::oid::Identifier;
use models::schema::{Precision, DEFAULT_CATALOG, DEFAULT_DATABASE};
use models::utils::now_timestamp_nanos;
use prost::Message;
use protocol_parser::line_protocol::parser::Parser;
use protos::jaeger_api_v2::{KeyValue, Span};
use protos::jaeger_storage_v1::span_writer_plugin_server::SpanWriterPlugin;
use protos::jaeger_storage_v1::{
    CloseWriterRequest, CloseWriterResponse, WriteSpanRequest, WriteSpanResponse,
};
use spi::server::dbms::DBMSRef;
use tonic::{Response, Status};

const TABLE_FIELD: &str = "table";
const USERNAME_FIELD: &str = "user";
const PASSWORD_FIELD: &str = "password";
pub const JAEGER_TRACE_TABLE: &str = "jaeger_trace";

pub struct JaegerWriteService {
    coord: CoordinatorRef,
    dbms: DBMSRef,
}

impl JaegerWriteService {
    pub fn new(coord: CoordinatorRef, dbms: DBMSRef) -> Self {
        Self { coord, dbms }
    }

    pub async fn get_tenant_db_and_check_privilege(&self, span: &Span) -> Result<(), Status> {
        let user = if let Some(kv) = span.tags.iter().find(|tag| tag.key == USERNAME_FIELD) {
            kv.v_str.clone()
        } else {
            ROOT.to_string()
        };
        let password = if let Some(kv) = span.tags.iter().find(|tag| tag.key == PASSWORD_FIELD) {
            kv.v_str.clone()
        } else {
            ROOT_PWD.to_string()
        };
        self.privilege_check(DEFAULT_CATALOG, DEFAULT_DATABASE, &user, &password)
            .await?;
        Ok(())
    }

    async fn privilege_check(
        &self,
        tenant: &str,
        db: &str,
        user: &str,
        password: &str,
    ) -> Result<(), Status> {
        let tenant_id = *self
            .coord
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| Status::invalid_argument("invalid tenant"))?
            .tenant()
            .id();
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(DatabasePrivilege::Write, Some(db.to_string())),
            Some(tenant_id),
        );
        let user_info = UserInfo {
            user: user.to_string(),
            password: password.to_string(),
            private_key: None,
        };
        let user = self
            .dbms
            .authenticate(&user_info, tenant)
            .await
            .map_err(|e| Status::permission_denied(e.to_string()))?;
        if !user.check_privilege(&privilege) {
            return Err(Status::permission_denied(format!(
                "user {} has no privilege {:?}",
                user_info.user, privilege
            )));
        }
        Ok(())
    }

    fn parse_kv(&self, prefix: &str, kv: &KeyValue) -> String {
        let mut line = String::new();
        let key = prefix.to_owned() + &kv.key;
        let mut buf = Vec::new();
        kv.encode(&mut buf).expect("serialize key value failed");
        let value = buf
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join("_");
        line.push_str(&format!("{}=\"{}\"", &key, value));
        line
    }

    fn parse_span(&self, span: &Span) -> String {
        // spans table
        let mut line_spans_tb = String::new();
        let table = JAEGER_TRACE_TABLE.to_string() + " ";
        line_spans_tb.push_str(&table);

        // tag

        // field
        let trace_id = span
            .trace_id
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join("_");
        line_spans_tb.push_str(&format!("trace_id=\"{}\",", trace_id));

        line_spans_tb.push_str(&format!("operation_name=\"{}\",", span.operation_name));

        if let Some(process) = span.process.as_ref() {
            line_spans_tb.push_str(&format!(
                "process.service_name=\"{}\",",
                process.service_name
            ));
        }

        line_spans_tb.push_str(&format!(
            "span_id=\"{}\",",
            span.span_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_")
        ));

        for (i, ref_) in span.references.iter().enumerate() {
            let trace_id = ref_
                .trace_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_");
            line_spans_tb.push_str(&format!("references.{}.trace_id=\"{}\",", i, trace_id));

            let span_id = ref_
                .span_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_");
            line_spans_tb.push_str(&format!("references.{}.span_id=\"{}\",", i, span_id));

            line_spans_tb.push_str(&format!("references.{}.ref_type=\"{}\",", i, ref_.ref_type));
        }

        line_spans_tb.push_str(&format!("flags=\"{}\",", span.flags));

        let duration = if let Some(dur) = span.duration.as_ref() {
            dur.seconds as f64 + dur.nanos as f64 / 1_000_000_000.0
        } else {
            0.0
        };
        line_spans_tb.push_str(&format!("duration={},", duration));

        span.tags.iter().for_each(|kv| {
            if kv.key != USERNAME_FIELD && kv.key != PASSWORD_FIELD {
                line_spans_tb.push_str(&self.parse_kv("tags.", kv));
                line_spans_tb.push(',');
            }
        });

        for (i, log) in span.logs.iter().enumerate() {
            if let Some(log_timestamp) = log.timestamp.as_ref() {
                let log_timestamp =
                    log_timestamp.seconds as f64 + log_timestamp.nanos as f64 / 1_000_000_000.0;
                line_spans_tb.push_str(&format!("logs.{}.timestamp={},", i, log_timestamp));
            }
            let prefix = format!("logs.{}.fields.", i);
            log.fields.iter().for_each(|kv| {
                line_spans_tb.push_str(&self.parse_kv(&prefix, kv));
                line_spans_tb.push(',');
            });
        }

        if let Some(process) = span.process.as_ref() {
            process.tags.iter().for_each(|kv| {
                line_spans_tb.push_str(&self.parse_kv("process.tags.", kv));
                line_spans_tb.push(',');
            })
        }

        line_spans_tb.push_str(&format!("process_id=\"{}\",", span.process_id));

        for (i, warning) in span.warnings.iter().enumerate() {
            line_spans_tb.push_str(&format!("warnings.{}=\"{}\",", i, warning));
        }

        line_spans_tb.pop();
        line_spans_tb.push(' ');

        // timestamp
        let timestamp = if let Some(time) = span.start_time.as_ref() {
            time.seconds * 1_000_000_000 + time.nanos as i64
        } else {
            now_timestamp_nanos()
        };
        line_spans_tb.push_str(&format!("{}", timestamp));

        line_spans_tb
    }
}

#[tonic::async_trait]
impl SpanWriterPlugin for JaegerWriteService {
    async fn write_span(
        &self,
        request: tonic::Request<WriteSpanRequest>,
    ) -> std::result::Result<tonic::Response<WriteSpanResponse>, tonic::Status> {
        let response = WriteSpanResponse {};
        if let Some(span) = request.into_inner().span {
            // check privilege
            self.get_tenant_db_and_check_privilege(&span)
                .await
                .map_err(|e| Status::permission_denied(e.to_string()))?;

            // parse span to line_str
            let line_str = self.parse_span(&span);

            // parse line_str to line
            let parser = Parser::new(Utc::now().timestamp_millis());
            let lines = parser
                .parse(&line_str)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

            self.coord
                .write_lines(
                    DEFAULT_CATALOG,
                    DEFAULT_DATABASE,
                    Precision::NS,
                    lines,
                    None,
                )
                .await
                .map_err(|e| {
                    Status::internal(format!("failed to write lines to database {}", e))
                })?;
        }

        Ok(Response::new(response))
    }

    async fn close(
        &self,
        _request: tonic::Request<CloseWriterRequest>,
    ) -> std::result::Result<tonic::Response<CloseWriterResponse>, tonic::Status> {
        let response = CloseWriterResponse {};

        Ok(Response::new(response))
    }
}
