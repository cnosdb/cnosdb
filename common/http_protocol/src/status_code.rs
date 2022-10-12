use reqwest::StatusCode;

pub const OK: StatusCode = StatusCode::OK;
/// 请求参数非法
pub const BAD_REQUEST: StatusCode = StatusCode::BAD_REQUEST;
/// 用户密码错误 或 用户不存在
pub const _UNAUTHORIZED: StatusCode = StatusCode::UNAUTHORIZED;
/// 路径不存在
pub const NOT_FOUND: StatusCode = StatusCode::NOT_FOUND;
/// 路径不支持对应的请求方式
pub const METHOD_NOT_ALLOWED: StatusCode = StatusCode::METHOD_NOT_ALLOWED;
/// 请求的消息体过大，超过限制
pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode::PAYLOAD_TOO_LARGE;
/// 操作执行失败
pub const UNPROCESSABLE_ENTITY: StatusCode = StatusCode::UNPROCESSABLE_ENTITY;

/// 查询超时或外部环境引起的异常
pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;
/// 服务不可用
pub const _SERVICE_UNAVAILABLE: StatusCode = StatusCode::SERVICE_UNAVAILABLE;
