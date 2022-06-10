# Get Started

## Requirements

**rust**

[https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)

We use `rustfmt` nightly version to format code. To install it:

```
rustup default nigntly
```

or

```
rustup toolchain install nightly
rustup component add rustfmt --toolchain nightly
```

**others**

flatbuffers: `brew install flatbuffers`

## Compile

```sh
cargo build
```
