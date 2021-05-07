use rusqlite::{Connection, NO_PARAMS};

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
    pub fn execute_string_stmt(&self, query: &str) -> Result<(), String> {
        return match self.get_conn().execute(query, NO_PARAMS) {
            Ok(_) => { Ok(()) }
            Err(err) => Err(sql_err(err))
        };
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

    // return the Connection object. panic if not available (e.g.: closed the database connection)
    fn get_conn(&self) -> &Connection {
        self.conn.as_ref().unwrap()
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
    eprintln!("rusqlite exception -- {}", err.to_string());
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constructor() {
        assert!(DB::open("sample.db").is_ok());
        assert!(DB::open_with_dsn("sample.db", "cache=shared&mode=memory").is_ok());
        assert!(DB::open_in_memory().is_ok());
        assert!(DB::open_in_memory_with_dsn("cache=shared&mode=memory").is_ok());

        let res = DB::open("etc/sample.db");
        assert!(res.is_err());
        assert_eq!(res.err().unwrap(), "unable to open database file: etc/sample.db")
    }

    #[test]
    fn test_execute_string_stmt() {
        let res = DB::open_in_memory();
        assert!(res.is_ok());

        let db = res.unwrap();
        let res = db.execute_string_stmt("CREATE TABLE IF NOT EXISTS cat_colors (
             id INTEGER PRIMARY KEY,
             name TEXT NOT NULL UNIQUE
         )");
        assert!(res.is_ok());

        assert!(db.close().is_ok());
    }

    #[test]
    fn test_fk_constraints() {
        let db = DB::open_in_memory().unwrap();

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
}
