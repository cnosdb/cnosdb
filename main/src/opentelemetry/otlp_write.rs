use std::collections::HashMap;

use prost::Message;
use protos::common::KeyValue as OtlpKeyValue;
use protos::trace::Span as OtlpSpan;

pub const JAEGER_TRACE_TABLE: &str = "jaeger_trace";

pub struct OtlpWrite {}

impl OtlpWrite {
    fn encode_kv(prefix: &str, kv: &OtlpKeyValue) -> String {
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

    fn kv_to_hashmap(tags: Vec<OtlpKeyValue>) -> HashMap<String, OtlpKeyValue> {
        let mut map = HashMap::new();
        for kv in tags {
            map.insert(kv.key.clone(), kv);
        }
        map
    }

    pub fn span_to_string(span: &OtlpSpan, table: String) -> String {
        // spans table
        let table = table + ",";
        let mut line_spans_tb = String::new();
        line_spans_tb.push_str(&table);

        // tag
        let trace_id: String = span
            .trace_id
            .iter()
            .fold(String::new(), |acc, byte| acc + &format!("{:02x}", byte));
        line_spans_tb.push_str(&format!("trace_id={},", trace_id));

        if !span.span_id.is_empty() {
            let span_id = span
                .span_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_");
            line_spans_tb.push_str(&format!("span_id={},", span_id));
        }

        if !span.parent_span_id.is_empty() {
            let parent_span_id = span
                .parent_span_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_");
            line_spans_tb.push_str(&format!("parent_span_id={},", parent_span_id));
        }

        if !span.trace_state.is_empty() {
            line_spans_tb.push_str(&format!("trace_state={},", span.trace_state));
        }

        if !span.name.is_empty() {
            line_spans_tb.push_str(&format!("name={},", span.name));
        }

        line_spans_tb.push_str(&format!("kind={},", span.kind));

        if let Some(status) = span.status.as_ref() {
            line_spans_tb.push_str(&format!("status_code=\"{}\",", status.code));
            line_spans_tb.push_str(&format!("status_message=\"{}\",", status.message));
        } else {
            line_spans_tb.push_str("status_code=\"\",");
            line_spans_tb.push_str("status_message=\"\",");
        }

        line_spans_tb.pop();
        line_spans_tb.push(' ');

        // field
        line_spans_tb.push_str(&format!(
            "end_time_unix_nano=\"{}\",",
            span.end_time_unix_nano
        ));

        span.attributes.iter().for_each(|kv| {
            line_spans_tb.push_str(&OtlpWrite::encode_kv("attributes.", kv));
            line_spans_tb.push(',');
        });

        line_spans_tb.push_str(&format!(
            "dropped_attributes_count={},",
            span.dropped_attributes_count
        ));

        for (i, event) in span.events.iter().enumerate() {
            line_spans_tb.push_str(&format!(
                "events.{}.time_unix_nano={},",
                i, event.time_unix_nano
            ));
            line_spans_tb.push_str(&format!("events.{}.name={},", i, event.name));
            line_spans_tb.push_str(&format!(
                "events.{}.dropped_attributes_count={},",
                i, event.dropped_attributes_count
            ));
            let prefix = format!("events.{}.attributes.", i);
            event.attributes.iter().for_each(|kv| {
                line_spans_tb.push_str(&OtlpWrite::encode_kv(&prefix, kv));
                line_spans_tb.push(',');
            });
        }

        line_spans_tb.push_str(&format!(
            "dropped_events_count={},",
            span.dropped_events_count
        ));

        for (i, link) in span.links.iter().enumerate() {
            let trace_id = link
                .trace_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_");
            line_spans_tb.push_str(&format!("links.{}.trace_id=\"{}\",", i, trace_id));

            let span_id = link
                .span_id
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join("_");
            line_spans_tb.push_str(&format!("links.{}.span_id=\"{}\",", i, span_id));

            line_spans_tb.push_str(&format!(
                "links.{}.trace_state=\"{}\",",
                i, link.trace_state
            ));

            link.attributes.iter().for_each(|kv| {
                line_spans_tb.push_str(&OtlpWrite::encode_kv(
                    &format!("links.{}.attributes.", i),
                    kv,
                ));
                line_spans_tb.push(',');
            });

            line_spans_tb.push_str(&format!(
                "links.{}.dropped_attributes_count={},",
                i, link.dropped_attributes_count
            ));

            line_spans_tb.push_str(&format!("links.{}.flags={},", i, link.flags));
        }

        line_spans_tb.push_str(&format!(
            "dropped_links_count={},",
            span.dropped_links_count
        ));

        line_spans_tb.push_str(&format!("flags={},", span.flags));

        line_spans_tb.pop();
        line_spans_tb.push(' ');

        // timestamp
        line_spans_tb.push_str(&format!("{}", span.start_time_unix_nano));

        line_spans_tb
    }
}
