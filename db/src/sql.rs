pub struct Request {
    pub transaction: bool,
    pub statements: Box<[Statement]>,
}

pub struct Statement {
    pub sql: String,
    pub parameters: Box<[Parameter]>,
}

pub struct Parameter {}

#[derive(Debug)]
// Response represents the outcome of an operation that changes rows.
pub struct Response {
    pub last_insert_id: i64,
    pub rows_affected: i64,
    pub error: String,
    pub time: f64,
}

// Rows represents the outcome of an operation that returns query data.
pub struct Rows {
    pub columns: Box<[String]>,
    pub types: Box<[String]>,
    pub values: Box<[i64]>,
    pub error: String,
    pub time: f64,
}
