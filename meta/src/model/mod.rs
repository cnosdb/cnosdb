use std::sync::Arc;

use self::meta_admin::AdminMeta;
use self::meta_tenant::TenantMeta;

pub mod meta_admin;
pub mod meta_tenant;

pub type MetaRef = Arc<AdminMeta>;
pub type MetaClientRef = Arc<TenantMeta>;
