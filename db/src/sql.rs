use serde::{Deserialize, Serialize};

pub struct Request {
    pub transaction: bool,
    pub statements: Box<[Statement]>,
}

pub struct Statement {
    pub sql: String,
    pub parameters: Box<[Parameter]>,
}

pub enum Parameter {
    Integer(i64),
    Real(f64),
    Text(String),
}

#[derive(Debug, Deserialize, Serialize)]
// Response represents the outcome of an operation that changes rows.
pub struct Response {
    pub last_insert_id: i64,
    pub rows_affected: i64,
}

#[derive(Debug, Deserialize, Serialize)]
// Rows represents the outcome of an operation that returns query data.
pub struct Rows<'a> {
    pub columns: Vec<String>,
    pub types: Vec<DataType>,
    #[serde(borrow)]
    pub values: Vec<Vec<Value<'a>>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Value<'a> {
    /// The value is a `NULL` value.
    Null,
    /// The value is a signed integer.
    Integer(i64),
    /// The value is a floating point number.
    Real(f64),
    /// The value is a text string.
    Text(String),
    /// The value is a blob of data
    Blob(&'a [u8]),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Null,
    Integer,
    Real,
    Text,
    Blob,
}