use models::define_result;
use snafu::Snafu;

pub mod privilege;
pub mod role;
pub mod role_manager;
pub mod user;
pub mod user_manager;

define_result!(AuthError);

#[derive(Debug, Snafu)]
pub enum AuthError {
    #[snafu(display("Generate id, error: {}", error))]
    IdGenerate { error: String },

    #[snafu(display("The tenant already exists"))]
    TenantAlreadyExists,

    #[snafu(display("The tenant not found"))]
    TenantNotFound,

    #[snafu(display("The member already exists in the tenant"))]
    MemberAlreadyExists,

    #[snafu(display("The member not found in the tenant"))]
    MemberNotFound,

    #[snafu(display("The role already exists in the tenant"))]
    RoleAlreadyExists,

    #[snafu(display("The role not found in the tenant"))]
    RoleNotFound,

    #[snafu(display("The privilege already exists in the role"))]
    PrivilegeAlreadyExists,

    #[snafu(display("The privilege not found in the role"))]
    PrivilegeNotFound,

    #[snafu(display("The user {} already exists", user))]
    UserAlreadyExists { user: String },

    #[snafu(display("The user {} not found", user))]
    UserNotFound { user: String },

    #[snafu(display(
        "Internal error: {}. This was likely caused by a bug in Cnosdb's \
    code and we would welcome that you file an bug report in our issue tracker",
        err
    ))]
    Internal { err: String },
}
