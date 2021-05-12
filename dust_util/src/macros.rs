/// Simulates Go's defer.
///
/// Please note that, different from go, this defer is bound to scope.
/// When exiting the scope, its deferred calls are executed in last-in-first-out order.
#[macro_export]
macro_rules! defer {
    ($t:expr) => {
        let __ctx = $crate::DeferContext::new(|| $t);
    };
}

/// A shortcut to box an error.
#[macro_export]
macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<dyn Error + Sync + Send> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::atomic::{Ordering, AtomicBool};
    use std::error::Error;

    #[test]
    fn test_defer() {
        let should_panic = Rc::new(AtomicBool::new(true));
        let sp = Rc::clone(&should_panic);
        defer!(assert!(!sp.load(Ordering::SeqCst)));
        should_panic.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_box_error() {
        let file_name = file!();
        let line_number = line!();
        let e: Box<dyn Error + Send + Sync> = box_err!("{}", "hi");
        assert_eq!(
            format!("{}", e),
            format!("[{}:{}]: hi", file_name, line_number + 1)
        );
    }
}
