use std::time::{Instant, Duration};

/// Encapsulates doing some work every time a timeout has elapsed
pub struct Ticker {
    last: Instant,
    timeout: Duration,
    remaining: Duration,
}

impl Ticker {
    pub fn new(period: Duration) -> Self {
        Ticker {
            last: Instant::now(),
            timeout: period,
            remaining: period,
        }
    }

    /// Do some work if the timeout has elapsed and return duration left until next timeout
    pub fn tick<T: FnMut()>(&mut self, mut callback: T) -> Duration {
        let elapsed = self.last.elapsed();
        if elapsed >= self.remaining {
            callback();
            self.last = Instant::now();
            self.remaining = self.timeout;
        } else {
            self.remaining -= elapsed;
        }
        self.remaining
    }
}
