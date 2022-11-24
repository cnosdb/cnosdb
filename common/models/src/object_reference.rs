#[derive(Debug, Clone, Copy)]
pub struct ResolvedObjectReference<'a> {
    pub parent: &'a str,
    pub name: &'a str,
}

/// Represents a path to a table that may require further resolution
#[derive(Debug, Clone, Copy)]
pub enum ObjectReference<'a> {
    Bare {
        name: &'a str,
    },
    /// A fully resolved role reference, e.g. "xx.name"
    Full {
        parent: &'a str,
        name: &'a str,
    },
}

impl<'a> ObjectReference<'a> {
    /// Retrieve the actual table name, regardless of qualification
    pub fn role(&self) -> &str {
        match self {
            Self::Full { name, .. } | Self::Bare { name } => name,
        }
    }

    /// Given a default catalog, ensure this object reference is fully resolved
    pub fn resolve(self, default_parent: &'a str) -> ResolvedObjectReference<'a> {
        match self {
            Self::Full { parent, name } => ResolvedObjectReference { parent, name },
            Self::Bare { name } => ResolvedObjectReference {
                parent: default_parent,
                name,
            },
        }
    }
}

impl<'a> From<&'a str> for ObjectReference<'a> {
    fn from(s: &'a str) -> Self {
        let parts: Vec<&str> = s.split('.').collect();

        match parts.len() {
            1 => Self::Bare { name: s },
            2 => Self::Full {
                parent: parts[0],
                name: parts[1],
            },
            _ => Self::Bare { name: s },
        }
    }
}

impl<'a> From<ResolvedObjectReference<'a>> for ObjectReference<'a> {
    fn from(resolved: ResolvedObjectReference<'a>) -> Self {
        Self::Full {
            parent: resolved.parent,
            name: resolved.name,
        }
    }
}
