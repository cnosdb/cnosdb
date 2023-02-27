use std::borrow::Cow;
use std::collections::btree_map::Iter;
use std::collections::BTreeMap;

pub type Label = (&'static str, Cow<'static, str>);

#[derive(Debug, Clone, Default, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Labels(pub BTreeMap<&'static str, Cow<'static, str>>);

impl Labels {
    pub fn extend(&mut self, labels: impl IntoIterator<Item = Label>) {
        self.0.extend(labels)
    }

    pub fn insert(&mut self, label: Label) {
        let (k, v) = label;
        self.0.insert(k, v);
    }

    pub fn iter(&self) -> Iter<&'static str, Cow<'_, str>> {
        self.0.iter()
    }
}

impl IntoIterator for Labels {
    type Item = Label;
    type IntoIter = std::collections::btree_map::IntoIter<&'static str, Cow<'static, str>>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, const N: usize> From<&'a [(&'static str, &'static str); N]> for Labels {
    fn from(iterator: &'a [(&'static str, &'static str); N]) -> Self {
        let labels = iterator
            .iter()
            .map(|(key, value)| {
                assert_legal_key(key);
                (*key, Cow::Borrowed(*value))
            })
            .collect();
        Self(labels)
    }
}

impl<const N: usize> From<[(&'static str, Cow<'static, str>); N]> for Labels {
    fn from(iterator: [(&'static str, Cow<'static, str>); N]) -> Self {
        Self(
            IntoIterator::into_iter(iterator)
                .map(|(key, value)| {
                    assert_legal_key(key);
                    (key, value)
                })
                .collect(),
        )
    }
}

impl<const N: usize> From<[(&'static str, u64); N]> for Labels {
    fn from(iterator: [(&'static str, u64); N]) -> Self {
        Self(
            IntoIterator::into_iter(iterator)
                .map(|(key, value)| {
                    assert_legal_key(key);
                    (key, Cow::Owned(value.to_string()))
                })
                .collect(),
        )
    }
}

impl<'a, const N: usize> From<[(&'static str, &'a str); N]> for Labels {
    fn from(iterator: [(&'static str, &'a str); N]) -> Self {
        Self(
            IntoIterator::into_iter(iterator)
                .map(|(key, value)| {
                    assert_legal_key(key);
                    (key, Cow::from(value.to_string()))
                })
                .collect(),
        )
    }
}

impl<const N: usize> From<[(&'static str, String); N]> for Labels {
    fn from(iterator: [(&'static str, String); N]) -> Self {
        Self(
            IntoIterator::into_iter(iterator)
                .map(|(key, value)| {
                    assert_legal_key(key);
                    (key, Cow::from(value))
                })
                .collect(),
        )
    }
}

pub fn assert_legal_key(s: &str) {
    assert!(!s.is_empty(), "string must not be empty");
    assert!(
        s.chars().all(|c| matches!(c, '0'..='9' | 'a'..='z' | '_')),
        "string must be [0-9a-z_]+ got: \"{s}\""
    )
}
