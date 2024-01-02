use datafusion::execution::context::SessionContext;
use spi::service::protocol::Context;

mod current_database;
mod current_role;
mod current_tenant;
mod current_user;

pub fn register_session_udfs(df_session_ctx: &SessionContext, context: &Context) {
    current_user::register_session_udf(df_session_ctx, context);
    current_tenant::register_session_udf(df_session_ctx, context);
    current_database::register_session_udf(df_session_ctx, context);
    current_role::register_session_udf(df_session_ctx, context);
}
