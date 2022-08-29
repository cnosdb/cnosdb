# Coding Specifications

# Coding Style

use `make fmt` to format code.

For things, you do not want rustfmt to mangle, use `#[rustfmt::skip]`

To prevent rustfmt from formatting a macro or an attribute, use `#[rustfmt::skip::macros(target_macro_name)]`
or `#[rustfmt::skip::attributes(target_attribute_name)]`

Example #1:

```rust
#![rustfmt::skip::attributes(custom_attribute)]

#[custom_attribute(formatting, here, should, be, Skipped)]
#[rustfmt::skip::macros(html)]
fn main() {
    let macro_result1 = html! { <div>
Hello</div>
    }.to_string();
```

Example #2:

```rust
fn main() {
    #[rustfmt::skip]
    let got = vec![
        0x00, 0x05, 0x01, 0x00,
        0xff,
        0x00,
        0x00,
        0x01, 0x0c, 0x02, 0x00,
        0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef,
        b'd', b'e', b'a', b'd', b'b', b'e', b'e', b'f', 0x00,
        0x00,
        127, 0x06, 0x03, 0x00,
        0x01, 0x02,
        b'a', b'b', b'c', b'd', 0x00,
        b'1', b'2', b'3', b'4', 0x00,
        0x00,
    ];
}
```

# Errors

# Tests