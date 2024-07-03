use std::collections::HashMap;

use chrono::Utc;
use coordinator::service::CoordinatorRef;
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::auth::user::{UserInfo, ROOT, ROOT_PWD};
use models::oid::Identifier;
use models::schema::database_schema::Precision;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE};
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

    async fn get_tenant_db_and_check_privilege(&self, span: &Span) -> Result<(), Status> {
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

    fn encode_kv(prefix: &str, kv: &KeyValue) -> String {
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

    fn kv_to_hashmap(tags: Vec<KeyValue>) -> HashMap<String, KeyValue> {
        let mut map = HashMap::new();
        for kv in tags {
            map.insert(kv.key.clone(), kv);
        }
        map
    }

    fn span_to_string(span: &Span) -> String {
        // tags kv to hashmap
        let tags_map = JaegerWriteService::kv_to_hashmap(span.tags.clone());

        // spans table
        let mut line_spans_tb = String::new();
        let table = JAEGER_TRACE_TABLE.to_string() + ",";
        line_spans_tb.push_str(&table);

        // tag
        let trace_id = span
            .trace_id
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join("_");
        line_spans_tb.push_str(&format!("trace_id={},", trace_id));

        line_spans_tb.push_str(&format!(
            "span_id={},",
            span.span_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_")
        ));

        line_spans_tb.push_str(&format!("operation_name={},", span.operation_name));

        if let Some(kv) = tags_map.get("span.kind") {
            line_spans_tb.push_str(&format!("tags.span.kind={},", kv.v_str));
        }

        if let Some(kv) = tags_map.get("otel.status_code") {
            line_spans_tb.push_str(&format!("tags.otel.status_code={},", kv.v_str));
        }

        if let Some(kv) = tags_map.get("otel.library.name") {
            line_spans_tb.push_str(&format!("tags.otel.library.name={},", kv.v_str));
        }

        if let Some(kv) = tags_map.get("otel.library.version") {
            line_spans_tb.push_str(&format!("tags.otel.library.version={},", kv.v_str));
        }

        line_spans_tb.pop();
        line_spans_tb.push(' ');

        // field
        if let Some(process) = span.process.as_ref() {
            line_spans_tb.push_str(&format!(
                "process.service_name=\"{}\",",
                process.service_name
            ));
        }

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
            if kv.key != USERNAME_FIELD
                && kv.key != PASSWORD_FIELD
                && kv.key != "otel.status_code"
                && kv.key != "span.kind"
                && kv.key != "otel.library.name"
                && kv.key != "otel.library.version"
            {
                line_spans_tb.push_str(&JaegerWriteService::encode_kv("tags.", kv));
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
                line_spans_tb.push_str(&JaegerWriteService::encode_kv(&prefix, kv));
                line_spans_tb.push(',');
            });
        }

        if let Some(process) = span.process.as_ref() {
            process.tags.iter().for_each(|kv| {
                line_spans_tb.push_str(&JaegerWriteService::encode_kv("process.tags.", kv));
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
            let line_str = JaegerWriteService::span_to_string(&span);

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
