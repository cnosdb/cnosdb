//! CatalogProvider:            ---> namespace
//! - SchemeProvider #1         ---> db
//!     - dyn tableProvider #1  ---> table
//!         - field #1
//!         - Column #2
//!     - dyn TableProvider #2
//!         - Column #3
//!         - Column #4

pub mod database_schema;
pub mod external_table_schema;
pub mod resource_info;
pub mod stream_table_schema;
pub mod table_schema;
pub mod tenant;
pub mod tskv_table_schema;
pub mod utils;

pub const TIME_FIELD_NAME: &str = "time";
pub const FIELD_ID: &str = "_field_id";
pub const TAG: &str = "_tag";
pub const DEFAULT_DATABASE: &str = "public";
pub const USAGE_SCHEMA: &str = "usage_schema";
pub const CLUSTER_SCHEMA: &str = "cluster_schema";
pub const DEFAULT_CATALOG: &str = "cnosdb";
pub const DEFAULT_PRECISION: &str = "NS";
pub const GIS_SRID_META_KEY: &str = "gis.srid";
pub const GIS_SUB_TYPE_META_KEY: &str = "gis.sub_type";
pub const COLUMN_ID_META_KEY: &str = "column_id";
