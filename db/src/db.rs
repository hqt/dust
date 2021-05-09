use rusqlite::{Connection, NO_PARAMS, Transaction};
use crate::sql::{Request, Response, Statement, Parameter};
use crate::sql;
use std::ops::Deref;

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

    // allows control of foreign key constraint checks.
    pub fn enable_fk_constraints(&self, flag: bool) -> Result<(), String> {
        let mut q = FK_CHECKS_ENABLED;
        if !flag {
            q = FK_CHECKS_DISABLED;
        }

        return match self.get_conn().execute(q, NO_PARAMS) {
            Ok(_) => { Ok(()) }
            Err(err) => Err(sql_err(err))
        };
    }

    // returns whether FK constraints are set or not.
    pub fn fk_constraints(&self) -> Result<bool, String> {
        let res: rusqlite::Result<i64> = self.get_conn().query_row(
            FK_CHECKS, NO_PARAMS, |r| r.get(0));
        return match res {
            Ok(check) => { Ok(check == 1) }
            Err(err) => Err(sql_err(err))
        };
    }

    // executes queries that modify the database.
    pub fn execute(&mut self, req: &Request) -> Result<Vec<Response>, String> {
        let rollback = false;
        let mut tx: Option<Transaction> = None;
        // defer!({
        //     if req.transaction {
        //         if rollback {
        //             tx.unwrap().rollback();
        //         } else {
        //            tx.unwrap().commit();
        //         }
        //     }
        // });

        let conn = self.get_mut_conn();

        // if req.transaction {
        //     tx = match conn.transaction() {
        //         Ok(transaction) => Option::Some(transaction),
        //         Err(err) => return Err(sql_err(err))
        //     };
        // }

        // trait Executor {
        //     fn insert_data<P>(&mut self, sql: &str, params: P) -> rusqlite::Result<usize>
        //         where P: IntoIterator,
        //     ;
        // }

        let mut results = Vec::new();
        // Execute each statement.
        for stmt in req.statements.deref() {
            let sql = &stmt.sql;
            if sql == "" {
                continue;
            }

            match conn.execute(sql, NO_PARAMS) {
                Ok(rows) => {
                    let res = Response {
                        last_insert_id: 0,
                        rows_affected: rows as i64,
                        error: "".to_string(),
                        time: 0.0,
                    };
                    results.push(res);
                }
                Err(err) => return Err(sql_err(err))
            }
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

    // #[test]
    // fn test_empty_stmt() {
    //     let mut db = DB::open_in_memory().unwrap();
    //     assert!(db.execute_string_stmt("").is_ok());
    //     println!("FUCK: {}", db.execute_string_stmt(";").err().unwrap());
    //     // assert!(db.execute_string_stmt(";").is_ok());
    // }

    #[test]
    fn test_execute_error() {
        let mut db = DB::open_in_memory().unwrap();
        assert!(db.execute_string_stmt("exception query").is_err());
    }

    #[test]
    fn test_execute_string_stmt() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)").is_ok());
        assert!(db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("fiona")"#).is_ok());
    }

    #[test]
    fn test_multi_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)").is_ok());

        let r = Request {
            transaction: false,
            statements: Box::new([
                Statement { sql: r#"INSERT INTO foo(name) VALUES("fiona")"#.to_string(), parameters: Box::new([]) },
                Statement { sql: r#"INSERT INTO foo(name) VALUES("dana")"#.to_string(), parameters: Box::new([]) },
            ]),
        };
        let res = db.execute(&r);
        assert!(res.is_ok());
    }

    #[test]
    fn test_single_multiline_stmt() {
        let mut db = DB::open_in_memory().unwrap();
        let r = &Request {
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
        assert!(db.execute(r).is_ok());

        assert!(db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("fiona")"#).is_ok());
    }

    #[test]
    fn test_execute_json_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt(r#"CREATE TABLE foo (c0 VARCHAR(36), c1 JSON, c2 NCHAR, c3 NVARCHAR, c4 CLOB)"#).is_ok());

        assert!(db.execute_string_stmt(r#"INSERT INTO foo(c0, c1, c2, c3, c4) VALUES("fiona", '{"mittens": "foobar"}', "bob", "dana", "declan")"#).is_ok());
    }

    #[test]
    fn test_join_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt(r#"CREATE TABLE names (id INTEGER NOT NULL PRIMARY KEY, name TEXT, ssn TEXT)"#).is_ok());
        assert!(db.execute_string_stmt(r#"CREATE TABLE staff (id INTEGER NOT NULL PRIMARY KEY, employer TEXT, ssn TEXT)"#).is_ok());

        let r = &Request {
            transaction: false,
            statements: Box::new([
                Statement { sql: r#"INSERT INTO "names" VALUES(1,'bob','123-45-678')"#.to_string(), parameters: Box::new([]) },
                Statement { sql: r#"INSERT INTO "names" VALUES(2,'tom','111-22-333')"#.to_string(), parameters: Box::new([]) },
                Statement { sql: r#"INSERT INTO "names" VALUES(3,'matt','222-22-333')"#.to_string(), parameters: Box::new([]) },
            ]),
        };
        assert!(db.execute(r).is_ok());

        assert!(db.execute_string_stmt(r#"INSERT INTO "staff" VALUES(1,'acme','222-22-333')"#).is_ok());
    }

    #[test]
    fn test_single_concat_stmts() {
        let mut db = DB::open_in_memory().unwrap();

        assert!(db.execute_string_stmt(r#"CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)"#).is_ok());
        assert!(db.execute_string_stmt(r#"INSERT INTO foo(name) VALUES("fiona")"#).is_ok());
    }
}
