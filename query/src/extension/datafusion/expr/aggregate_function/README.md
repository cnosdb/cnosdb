# How to add a aggregate function?

## Functions

This document describes the main concepts, features, and examples of the simple function API in Cnosdb.
For more real-world API usage examples, check cnosdb/query/src/extension/datafusion/expr/aggregate_function/example.rs.

Aggregate functions process a batch or rows and produce a vector of results.

To define a aggregate function, eg. MY_AVG. Make a struct of AggregateUDF.

```rust
pub struct AggregateUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    pub accumulator: AccumulatorFunctionImplementation,
    /// the accumulator's state's description as a function of the return type
    pub state_type: StateTypeFunction,
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

### function state type

This signature corresponds to which types an aggregator serializes its state, given its return datatype.

the return value's type of Accumulator::state

### function implementation

is a closure function that returns the implementation of Accumulator

```rust
/// An accumulator represents a stateful object that lives throughout the evaluation of multiple rows and
/// generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update_batch`
/// * convert its internal state to a vector of scalar values
/// * update its state from multiple accumulators' states via `merge_batch`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function should return a vector
    // of two values, sum and n.
    fn state(&self) -> Result<Vec<ScalarValue>>;

    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()>;

    /// updates the accumulator's state from a vector of states.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue>;
}
```

## Registration

Use aggregate_function::register_udafs to register a aggregate function into FunctionMetadataManager.

```rust
pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    // extend function...
    // eg.
    //   example::register_udaf(func_manager)?;
    example::register_udaf(func_manager)?;
    Ok(())
}
```

## Testing

reference arrow-datafusion/datafusion/core/tests/sql/udf.rs -> udf::simple_udaf

## Benchmarking

add to cnosdb/query/benches/aggregate_function.rs

## Documentin

Comment and PR descriptions are written.