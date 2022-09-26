# How to add a scalar function?

## Vector Functions

This document describes the main concepts, features, and examples of the simple function API in Cnosdb.
For more real-world API usage examples, check cnosdb/query/src/extension/datafusion/expr/scalar_function/example.rs.

Vector functions process a batch or rows and produce a vector of results.

To define a vector scalar function, eg. MY_FUNC. Make a struct of ScalarUDF.

```rust
pub struct ScalarUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    pub fun: ScalarFunctionImplementation,
}
```

### function name

a globally unique function name

### function signature

A function's type signature, which defines the function's supported argument types.

- Variadic(Vec<DataType>)

Arbitrary number of arguments of an common type out of a list of valid types.
A function such as `concat` is `Variadic(vec![DataType::Utf8, DataType::LargeUtf8])`.

- VariadicEqual

Arbitrary number of arguments of an arbitrary but equal type.
A function such as `array` is `VariadicEqual`.
The first argument decides the type used for coercion.

- Uniform(usize, Vec<DataType>)

Fixed number of arguments of an arbitrary but equal type out of a list of valid types.
A function of one argument of f64 is `Uniform(1, vec![DataType::Float64])`.
A function of one argument of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`.

- Exact(Vec<DataType>)

Exact number of arguments of an exact type.

- Any(usize)

Fixed number of arguments of arbitrary types.

- OneOf(Vec<TypeSignature>)

One of a list of signatures

### function return type

ScalarUDF's return_type is a function, this function's input is ScalarUDF's input data type list, this function's result is ScalarUDF's actual return data type.

### function implementation

The implementation logic of a scalar function can be constructed with the help of **functions::make_scalar_function**.

notes:
> The number of input records must matches the number of output records, if the input is null, the corresponding output is null.
> Both input and output results are arrow structs.

## Registration

Use scalar_function::register_udfs to register a stateless vector function into FunctionMetadataManager.

```rust
pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    // extend function...
    // eg.
    //   example::register_udf(func_manager)?;
    example::register_udf(func_manager)?;
    Ok(())
}
```

## Testing

reference  arrow-datafusion/datafusion/core/tests/sql/udf.rs -> udf::scalar_udf

## Benchmarking

add to cnosdb/query/benches/scalar_function.rs

## Documentin

Comment and PR descriptions are written.