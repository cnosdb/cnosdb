use std::borrow::Cow;
use std::mem;

use minitrace::collector::SpanContext;
use minitrace::{Event, Span};

pub trait SpanExt {
    fn context(&self) -> Option<SpanContext>;
    fn from_context(name: impl Into<Cow<'static, str>>, context: Option<&SpanContext>) -> Self;
    fn add_property<K, V, F>(&mut self, property: F)
    where
        K: Into<Cow<'static, str>>,
        V: Into<Cow<'static, str>>,
        F: FnOnce() -> (K, V);

    fn add_properties<K, V, I, F>(&mut self, properties: F)
    where
        K: Into<Cow<'static, str>>,
        V: Into<Cow<'static, str>>,
        I: IntoIterator<Item = (K, V)>,
        F: FnOnce() -> I;
    fn error(&self, name: impl Into<Cow<'static, str>>);
    fn ok(&self, name: impl Into<Cow<'static, str>>);
}

impl SpanExt for Span {
    fn context(&self) -> Option<SpanContext> {
        SpanContext::from_span(self)
    }
    fn from_context(name: impl Into<Cow<'static, str>>, context: Option<&SpanContext>) -> Self {
        match context {
            Some(ctx) => Span::root(name, *ctx),
            None => Span::noop(),
        }
    }
    fn add_property<K, V, F>(&mut self, property: F)
    where
        K: Into<Cow<'static, str>>,
        V: Into<Cow<'static, str>>,
        F: FnOnce() -> (K, V),
    {
        let mut tmp = mem::take(self);
        tmp = tmp.with_property(property);
        mem::swap(self, &mut tmp);
    }

    fn add_properties<K, V, I, F>(&mut self, properties: F)
    where
        K: Into<Cow<'static, str>>,
        V: Into<Cow<'static, str>>,
        I: IntoIterator<Item = (K, V)>,
        F: FnOnce() -> I,
    {
        let mut tmp = mem::take(self);
        tmp = tmp.with_properties(properties);
        mem::swap(self, &mut tmp);
    }

    // TODO: display in jaeger
    fn error(&self, name: impl Into<Cow<'static, str>>) {
        Event::add_to_parent(name, self, || [("type".into(), "error".into())]);
    }

    // TODO: display in jaeger
    fn ok(&self, name: impl Into<Cow<'static, str>>) {
        Event::add_to_parent(name, self, || [("type".into(), "ok".into())]);
    }
}
