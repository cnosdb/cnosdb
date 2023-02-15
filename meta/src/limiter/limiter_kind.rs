use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum RequestLimiterKind {
    DataIn,
    DataOut,
    Queries,
    Writes,
}
impl Display for RequestLimiterKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
