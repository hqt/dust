use rusqlite::{Connection, NO_PARAMS};

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
            Err(err) => {
                eprintln!("{}", err);
                Err(format!("cannot create db path {}", path))
            }
        };
    }

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
            Err(err) => {
                eprintln!("execute error. {}", err);
                Err(String::from("execute error"))
            }
        };
    }

    fn get_conn(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

fn format_dsn(path: &str, dsn: &str) -> String {
    if dsn != "" {
        return format!("file:{}?{}", path, dsn);
    }
    return path.to_string();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constructor() {
        assert!(DB::open("etc/sample.db").is_err());
        assert!(DB::open("sample.db").is_ok());
        assert!(DB::open_with_dsn("sample.db", "cache=shared&mode=memory").is_ok());
        assert!(DB::open_in_memory().is_ok());
        assert!(DB::open_in_memory_with_dsn("cache=shared&mode=memory").is_ok());
    }

    #[test]
    fn test_execute_string_stmt() {
        let res = DB::open_in_memory();
        assert!(res.is_ok());

        let db = res.unwrap();
        let res = db.execute_string_stmt("create table if not exists cat_colors (
             id integer primary key,
             name text not null unique
         )");
        assert!(res.is_ok());

        assert!(db.close().is_ok());
    }
}

