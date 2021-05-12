use rusqlite::{Connection, ToSql};
use crate::sql::{Request, Response, Statement, Rows, Value, DataType, Parameter};
use std::ops::{Deref};
use rusqlite::types::ValueRef;
use std::str;

const FK_CHECKS: &str = "PRAGMA foreign_keys";
const FK_CHECKS_ENABLED: &str = "PRAGMA foreign_keys=ON";
const FK_CHECKS_DISABLED: &str = "PRAGMA foreign_keys=OFF";

// DB represents a wrapper for the Sqlite database instance
pub struct DB {
    conn: Option<Connection>,
}

impl DB {
    // opens a file-based database, creating it if it does not exist.
    pub fn open(path: &str) -> Result<DB, String> {
        return DB::new(format_dsn(path, "").as_str());
    }

    // opens a file-based database, creating it if it does not exist.
    pub fn open_with_dsn(path: &str, dsn: &str) -> Result<DB, String> {
        return DB::new(format_dsn(path, dsn).as_str());
    }

    // opens an in-memory database
    pub fn open_in_memory() -> Result<DB, String> {
        return DB::new(format_dsn(":memory:", "").as_str());
    }

    // opens an in-memory database
    pub fn open_in_memory_with_dsn(dsn: &str) -> Result<DB, String> {
        return DB::new(format_dsn(":memory:", dsn).as_str());
    }

    fn new(path: &str) -> Result<DB, String> {
        return match Connection::open(format_dsn(path, "")) {
            Ok(conn) => Ok(DB { conn: Some(conn) }),
            Err(err) => Err(sql_err(err))
        };
    }

    // closes the underlying database connection.
    pub fn close(mut self) -> Result<(), String> {
        if let Some(conn) = self.conn.take() {
            return match conn.close() {
                Ok(_) => { Ok(()) }
                Err((conn, err)) => {
                    eprintln!("{}", err);
                    self.conn = Some(conn);
                    Err(String::from("cannot create db path"))
                }
            };
        }

        Err(String::from("db connection is already closed"))
    }

    // allows control of foreign key constraint checks.
    pub fn enable_fk_constraints(&self, flag: bool) -> Result<(), String> {
        let mut q = FK_CHECKS_ENABLED;
        if !flag {
            q = FK_CHECKS_DISABLED;
        }

        return match self.get_conn().execute(q, []) {
            Ok(_) => { Ok(()) }
            Err(err) => Err(sql_err(err))
        };
    }

    // returns whether FK constraints are set or not.
    pub fn fk_constraints(&self) -> Result<bool, String> {
        let res: rusqlite::Result<i64> = self.get_conn().query_row(
            FK_CHECKS, [], |r| r.get(0));
        return match res {
            Ok(check) => { Ok(check == 1) }
            Err(err) => Err(sql_err(err))
        };
    }

    // execute_string_stmt executes a single query that modifies the database.
    // This is primarily a convenience function.
    pub fn execute_string_stmt(&mut self, query: &str) -> Result<Vec<Response>, String> {
        let stmt = Statement { sql: query.parse().unwrap(), parameters: Box::new([]) };
        let r = Request {
            transaction: false,
            statements: Box::new([stmt]),
        };
        return self.execute(&r);
    }

    // executes queries that modify the database.
    pub fn execute(&mut self, req: &Request) -> Result<Vec<Response>, String> {
        return match self._execute(req) {
            Ok(results) => { Ok(results) }
            Err(err) => { Err(sql_err(err)) }
        };
    }

    // internal implementation of execute that returns rusqlite::Error
    fn _execute(&mut self, req: &Request) -> Result<Vec<Response>, rusqlite::Error> {
        let conn = self.get_mut_conn();

        let mut results = Vec::new();
        // Execute each statement.
        for stmt in req.statements.deref() {
            let sql = &stmt.sql;
            if sql == "" {
                continue;
            }

            let params = &parameters(&stmt.parameters)[..];
            let rows_affected = conn.execute(sql, params)?;
            let last_insert_id = conn.last_insert_rowid();

            results.push(Response {
                last_insert_id,
                rows_affected: rows_affected as i64,
            });
        }

        Ok(results)
    }

    // executes a single query that return rows, but don't modify database.
    pub fn query_string_stmt(&self, query: &str) -> Result<Vec<Rows>, String> {
        let stmt = Statement { sql: query.parse().unwrap(), parameters: Box::new([]) };
        let r = Request {
            transaction: false,
            statements: Box::new([stmt]),
        };

        return self.query(&r);
    }

    // query executes queries that return rows, but don't modify the database.
    pub fn query(&self, req: &Request) -> Result<Vec<Rows>, String> {
        return match self._query(req) {
            Ok(results) => { Ok(results) }
            Err(err) => { Err(sql_err(err)) }
        };
    }

    // internal implementation of query that returns rusqlite::Error
    fn _query(&self, req: &Request) -> Result<Vec<Rows>, rusqlite::Error> {
        let conn = self.get_conn();
        let mut results = Vec::new();
        for stmt in req.statements.deref() {
            let sql = &stmt.sql;
            if sql == "" {
                continue;
            }

            let mut columns = Vec::new();
            let mut types = Vec::new();

            // a closure (for capturing the columns variable)
            // which maps from a single row under database to Vec<Value>: each index represents a single column of that row
            let mapper = |row: &rusqlite::Row| -> rusqlite::Result<Vec<Value>> {
                // get all column name and type once
                if columns.is_empty() {
                    columns = row.column_names().into_iter().map(|name| name.to_string()).collect();
                    types = (0..row.column_count()).into_iter().map(|i| {
                        return match row.get_ref_unwrap(i) {
                            ValueRef::Null => { DataType::Null }
                            ValueRef::Integer(_) => { DataType::Integer }
                            ValueRef::Real(_) => { DataType::Real }
                            ValueRef::Text(_) => { DataType::Text }
                            ValueRef::Blob(_) => { DataType::Blob }
                        };
                    }).collect();
                }

                let values = (0..row.column_count())
                    .into_iter()
                    .map(|i| {
                        return match row.get_ref_unwrap(i) {
                            ValueRef::Null => { Value::Null }
                            ValueRef::Integer(val) => { Value::Integer(val) }
                            ValueRef::Real(val) => { Value::Real(val) }
                            ValueRef::Text(val) => { Value::Text(str::from_utf8(val).unwrap().to_string()) }
                            // TODO clone &[u8] array
                            ValueRef::Blob(_) => { Value::Null }
                        };
                    })
                    .collect();

                Ok(values)
            };

            let params = &parameters(&stmt.parameters)[..];
            let mut prepare_stmt = conn.prepare(sql)?;
            let rows = prepare_stmt.query_map(params, mapper)?;
            let values = rows.into_iter().map(|r| r.unwrap()).collect();

            results.push(Rows {
                columns,
                types,
                values,
            });
        }

        Ok(results)
    }

    // return the Connection object. panic if not available (e.g.: closed the database connection)
    fn get_conn(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }

    // return the Connection object which can be mutable. panic if not available (e.g.: closed the database connection)
    fn get_mut_conn(&mut self) -> &mut Connection {
        self.conn.as_mut().unwrap()
    }
}

// returns the fully-qualified datasource name.
fn format_dsn(path: &str, dsn: &str) -> String {
    if dsn != "" {
        return format!("file:{}?{}", path, dsn);
    }
    return path.to_string();
}

// convert parameters to the suitable format for rustqlite
fn parameters(parameters: &Box<[Parameter]>) -> Vec<&dyn ToSql> {
    let params: Vec<&dyn ToSql> = parameters.iter().map(|p| {
        return match p {
            Parameter::Integer(x) => { x as &dyn ToSql }
            Parameter::Real(x) => { x as &dyn ToSql }
            Parameter::Text(x) => { x as &dyn ToSql }
        };
    }).collect();
    return params;
}

fn sql_err(err: rusqlite::Error) -> String {
    eprintln!("rusqlite exception -- {:?}", err);
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initialise_db() {
        // test create and close db
        let res = DB::open("sample.db");
        assert!(res.is_ok());
        assert!(res.unwrap().close().is_ok());

        // test other constructors
        assert!(DB::open_with_dsn("sample.db", "cache=shared&mode=memory").is_ok());
        assert!(DB::open_in_memory().is_ok());
        assert!(DB::open_in_memory_with_dsn("cache=shared&mode=memory").is_ok());

        // test error when initialising db
        let res = DB::open("etc/sample.db");
        assert!(res.is_err());
        assert_eq!(res.err().unwrap(), "unable to open database file: etc/sample.db")
    }

    #[test]
    fn test_fk_constraints() {
        let mut db = DB::open_in_memory().unwrap();

        // create table with FK constraint
        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, ref INTEGER REFERENCES foo(id))").is_ok());

        // test disable FK constraint: be able to insert data
        assert!(db.enable_fk_constraints(false).is_ok());
        assert_eq!(db.fk_constraints().unwrap(), false);
        assert!(db.execute_string_stmt("INSERT INTO foo(id, ref) VALUES(1, 2)").is_ok());

        // test enable FK constraint: cannot insert data
        assert!(db.enable_fk_constraints(true).is_ok());
        assert_eq!(db.fk_constraints().unwrap(), true);
        let res = db.execute_string_stmt("INSERT INTO foo(id, ref) VALUES(1, 3)");
        assert!(res.is_err());
        assert_eq!(res.err().unwrap(), "UNIQUE constraint failed: foo.id");
    }

    #[test]
    fn test_empty_stmt() {
        let mut db = DB::open_in_memory().unwrap();
        assert!(db.execute_string_stmt("").is_ok());
    }

    #[test]
    fn test_execute_error() {
        let mut db = DB::open_in_memory().unwrap();
        assert!(db.execute_string_stmt("exception query").is_err());
    }

    #[test]
    fn test_execute_success() {
        let mut db = DB::open_in_memory().unwrap();
        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)").is_ok());

        let r = db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("fiona")"#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"last_insert_id":1,"rows_affected":1}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let r = db.execute_string_stmt(r#"UPDATE foo SET name="dana" WHERE ID=1"#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"last_insert_id":1,"rows_affected":1}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_simple_string_stmt() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)").is_ok());

        assert!(db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("fiona")"#).is_ok());
        assert!(db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("aoife")"#).is_ok());

        let r = db.query_string_stmt("SELECT * FROM foo");
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"aoife"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let r = db.query_string_stmt(r#"SELECT * FROM foo WHERE name="aoife""#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let r = db.query_string_stmt(r#"SELECT * FROM foo WHERE name="unknown""#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":[],"types":[],"values":[]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let r = db.query_string_stmt(r#"SELECT * FROM foo ORDER BY name"#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"],[1,"fiona"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let r = db.query_string_stmt(r#"SELECT *,name FROM foo"#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name","name"],"types":["integer","text","text"],"values":[[1,"fiona","fiona"],[2,"aoife","aoife"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_simple_json_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt(r#"CREATE TABLE foo (c0 VARCHAR(36), c1 JSON, c2 NCHAR, c3 NVARCHAR, c4 CLOB)"#).is_ok());
        assert!(db.execute_string_stmt(r#"INSERT INTO foo(c0, c1, c2, c3, c4) VALUES("fiona", '{"mittens": "foobar"}', "bob", "dana", "declan")"#).is_ok());

        //  TODO rustqlite doesn't support returning other types than SQLite base types
        // let r = db.query_string_stmt(r#"SELECT * FROM foo"#);
        // assert!(r.is_ok());
        // assert_eq!(
        //     r#"[{"columns":["c0","c1","c2","c3","c4"],"types":["varchar(36)","json","nchar","nvarchar","clob"],"values":[["fiona","{\"mittens\": \"foobar\"}","bob","dana","declan"]]}]"#,
        //     serde_json::to_string(&r.unwrap()).unwrap()
        // );
    }

    #[test]
    fn test_simple_join_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt(r#"CREATE TABLE names (id INTEGER NOT NULL PRIMARY KEY, name TEXT, ssn TEXT)"#).is_ok());
        assert!(db.execute_string_stmt(r#"CREATE TABLE staff (id INTEGER NOT NULL PRIMARY KEY, employer TEXT, ssn TEXT)"#).is_ok());

        let req = &Request {
            transaction: false,
            statements: Box::new([
                Statement { sql: r#"INSERT INTO "names" VALUES(1,'bob','123-45-678')"#.to_string(), parameters: Box::new([]) },
                Statement { sql: r#"INSERT INTO "names" VALUES(2,'tom','111-22-333')"#.to_string(), parameters: Box::new([]) },
                Statement { sql: r#"INSERT INTO "names" VALUES(3,'matt','222-22-333')"#.to_string(), parameters: Box::new([]) },
            ]),
        };
        assert!(db.execute(req).is_ok());

        assert!(db.execute_string_stmt(r#"INSERT INTO "staff" VALUES(1,'acme','222-22-333')"#).is_ok());

        let r = db.query_string_stmt(r#"SELECT names.id,name,names.ssn,employer FROM names INNER JOIN staff ON staff.ssn = names.ssn"#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name","ssn","employer"],"types":["integer","text","text","text"],"values":[[3,"matt","222-22-333","acme"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_single_concat_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt(r#"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"#).is_ok());
        assert!(db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("fiona")"#).is_ok());

        let r = db.query_string_stmt(r#"SELECT id || "_bar", name FROM foo"#);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id || \"_bar\"","name"],"types":["text","text"],"values":[["1_bar","fiona"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_simple_multi_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)").is_ok());

        let req = &Request {
            transaction: false,
            statements: Box::new([
                Statement {
                    sql: r#"INSERT INTO foo(name) VALUES("fiona")"#.to_string(),
                    parameters: Box::new([]),
                },
                Statement {
                    sql: r#"INSERT INTO foo(name) VALUES("dana")"#.to_string(),
                    parameters: Box::new([]),
                },
            ]),
        };
        let r = db.execute(req);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let req = &Request {
            transaction: false,
            statements: Box::new([
                Statement { sql: r#"SELECT * FROM foo"#.to_string(), parameters: Box::new([]) },
                Statement { sql: r#"SELECT * FROM foo"#.to_string(), parameters: Box::new([]) },
            ]),
        };

        let r = db.query(req);
        assert!(r.is_ok());
        assert_eq!(
            concat!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"dana"]]},"#,
            r#"{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"dana"]]}]"#
            ),
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_single_multiline_stmt() {
        let mut db = DB::open_in_memory().unwrap();

        let req = &Request {
            transaction: false,
            statements: Box::new([Statement {
                sql: "
                CREATE TABLE foo (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT
                )".to_string(),
                parameters: Box::new([]),
            }]),
        };
        assert!(db.execute(req).is_ok());

        let req = &Request {
            transaction: false,
            statements: Box::new([
                Statement {
                    sql: r#"INSERT INTO foo(name) VALUES("fiona")"#.to_string(),
                    parameters: Box::new([]),
                },
                Statement {
                    sql: r#"INSERT INTO foo(name) VALUES("dana")"#.to_string(),
                    parameters: Box::new([]),
                },
            ]),
        };
        let r = db.execute(req);
        assert_eq!(
            r#"[{"last_insert_id":1,"rows_affected":1},{"last_insert_id":2,"rows_affected":1}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_parameterized_all_type_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INT, money FLOAT)").is_ok());

        let req = Request {
            transaction: false,
            statements: Box::new([
                Statement {
                    sql: "INSERT INTO foo(name, age, money) VALUES(?, ?, ?)".to_string(),
                    parameters: Box::new([
                        Parameter::Text("fiona".to_string()),
                        Parameter::Integer(20),
                        Parameter::Real(100.75),
                    ]),
                }
            ]),
        };
        assert!(db.execute(&req).is_ok());

        let r = db.query_string_stmt("SELECT * FROM foo");
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name","age","money"],"types":["integer","text","integer","real"],"values":[[1,"fiona",20,100.75]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );
    }

    #[test]
    fn test_simple_parameterized_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)").is_ok());

        let mut req = Request {
            transaction: false,
            statements: Box::new([
                Statement {
                    sql: "INSERT INTO foo(name) VALUES(?)".to_string(),
                    parameters: Box::new([
                        Parameter::Text("fiona".to_string()),
                    ]),
                }
            ]),
        };
        assert!(db.execute(&req).is_ok());

        req.statements[0].parameters[0] = Parameter::Text("aoife".to_string());
        assert!(db.execute(&req).is_ok());

        let r = db.query_string_stmt("SELECT * FROM foo");
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"aoife"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        req.statements[0].sql = "SELECT * FROM foo WHERE name=?".to_string();
        req.statements[0].parameters[0] = Parameter::Text("aoife".to_string());
        let r = db.query(&req);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        req.statements[0].parameters[0] = Parameter::Text("fiona".to_string());
        let r = db.query(&req);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        );

        let req = Request {
            transaction: false,
            statements: Box::new([
                Statement {
                    sql: "SELECT * FROM foo WHERE NAME=?".to_string(),
                    parameters: Box::new([
                        Parameter::Text("fiona".to_string()),
                    ]),
                },
                Statement {
                    sql: "SELECT * FROM foo WHERE NAME=?".to_string(),
                    parameters: Box::new([
                        Parameter::Text("aoife".to_string()),
                    ]),
                },
            ]),
        };
        let r = db.query(&req);
        assert!(r.is_ok());
        assert_eq!(
            r#"[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]},{"columns":["id","name"],"types":["integer","text"],"values":[[2,"aoife"]]}]"#,
            serde_json::to_string(&r.unwrap()).unwrap()
        )
    }
}
