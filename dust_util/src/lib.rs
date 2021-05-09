mod macros;

// Invokes the wrapped closure when dropped.
pub struct DeferContext<T: FnOnce()> {
    t: Option<T>,
}

impl<T: FnOnce()> DeferContext<T> {
    pub fn new(t: T) -> DeferContext<T> {
        DeferContext { t: Some(t) }
    }
}

impl<T: FnOnce()> Drop for DeferContext<T> {
    fn drop(&mut self) {
        self.t.take().unwrap()()
    }
}

pub struct Conn {}

impl Conn {
    fn fuck(&self) {}
}

pub struct Transaction<'conn> {
    conn: &'conn Conn,
}

impl Transaction<'_> {}

pub fn test() {
    let c = Transaction { conn: &Conn {} };
    c.conn.fuck();
    // c.fuck();
}