use openssl::error::ErrorStack;
use snafu::Snafu;

use crate::auth::privilege::DatabasePrivilege;
use crate::define_result;

pub mod privilege;
pub mod role;
pub mod rsa_utils;
pub mod user;

define_result!(AuthError);

#[derive(Debug, Snafu)]
pub enum AuthError {
    #[snafu(display("Generate id, error: {}", error))]
    IdGenerate { error: String },

    #[snafu(display("Rsa error: {}", source))]
    Rsa { source: ErrorStack },

    #[snafu(display("Password not set"))]
    PasswordNotSet,

    #[snafu(display("Access denied for user '{}' (using {}) {}", user_name, auth_type, err))]
    AccessDenied {
        user_name: String,
        auth_type: String,
        err: String,
    },

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

    #[snafu(display("The privilege {:?} of {} not found in the role", privilege, db))]
    PrivilegeNotFound {
        db: String,
        privilege: DatabasePrivilege,
        role: String,
    },

    #[snafu(display("The user {} already exists", user))]
    UserAlreadyExists { user: String },

    #[snafu(display("The user {} not found", user))]
    UserNotFound { user: String },

    #[snafu(display("{}", err))]
    Metadata { err: String },

    #[snafu(display(
        "Internal error: {}. This was likely caused by a bug in Cnosdb's \
    code and we would welcome that you file an bug report in our issue tracker",
        err
    ))]
    Internal { err: String },
}
