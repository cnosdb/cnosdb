use derive_traits::{ErrorCode, ErrorCoder};
use snafu::Snafu;

#[derive(ErrorCoder, Snafu, Debug)]
#[error_code(mod_code = "00")]
enum TestError1 {
    #[error_code(code = 1)]
    #[snafu(display("a"))]
    A1,

    #[error_code(code = 2)]
    #[snafu(display("b"))]
    B1,

    #[error_code(code = 3)]
    #[snafu(display("c"))]
    C1,
}

#[test]
fn test1() {
    assert_eq!(TestError1::A1.code(), "000001");
    assert_eq!(TestError1::code_a_1(), "000001");

    assert_eq!(TestError1::B1.code(), "000002");
    assert_eq!(TestError1::code_b_1(), "000002");

    assert_eq!(TestError1::C1.code(), "000003");
    assert_eq!(TestError1::code_c_1(), "000003");
}

#[derive(ErrorCoder, Snafu, Debug)]
#[error_code(mod_code = "xx")]
enum TestError2 {
    A2,
    B2,
}

#[test]
fn test2() {
    let a = TestError2::A2;
    assert_eq!(a.code(), "xx0000");
    let b = TestError2::B2;
    assert_eq!(b.code(), "xx0000");
}

#[derive(ErrorCoder, Snafu, Debug)]
enum TestError3 {
    #[error_code(code = 1)]
    A3,

    B3 {
        source: std::io::Error,
    },
}

#[test]
fn test3() {
    let a = TestError3::A3;
    assert_eq!(a.code(), "000001");
    assert_eq!(TestError3::code_a_3(), "000001");

    let b = TestError3::B3 {
        source: std::io::Error::new(std::io::ErrorKind::AddrInUse, "test"),
    };
    assert_eq!(b.code(), "000000");
}

#[derive(ErrorCoder, Snafu, Debug)]
enum TestError4 {
    #[error_code(code = 2)]
    A4,

    #[error_code(code = 1)]
    B4,
}

#[test]
fn test4() {
    assert_eq!(TestError4::A4.code(), "000002");
    assert_eq!(TestError4::code_a_4(), "000002");

    assert_eq!(TestError4::B4.code(), "000001");
    assert_eq!(TestError4::code_b_4(), "000001");
}
