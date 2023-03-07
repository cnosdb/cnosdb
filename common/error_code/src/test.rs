use snafu::Snafu;

use crate::{ErrorCode, ErrorCoder};

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
    let a = TestError1::A1;
    let b = TestError1::B1;
    let c = TestError1::C1;
    assert_eq!(a.code(), "000001");
    assert_eq!(b.code(), "000002");
    assert_eq!(c.code(), "000003");
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
    let b = TestError3::B3 {
        source: std::io::Error::new(std::io::ErrorKind::AddrInUse, "test"),
    };
    assert_eq!(a.code(), "000001");
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
    let a = TestError4::A4;
    let b = TestError4::B4;

    println!("{}", a.code());
    println!("{}", b.code());
}
