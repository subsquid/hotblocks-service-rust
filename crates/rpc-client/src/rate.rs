/// Sliding-window rate meter.
///
/// Ports `util/rpc-client/src/rate.ts` exactly:
/// - `window_size` slots (default 10) each `slot_time` ms wide (default 100ms)
/// - `inc(count, now)` records `count` events at time `now`
/// - `get_rate(now)` returns the rolling rate in events/second over the last
///   `window_size * slot_time` milliseconds
pub struct RateMeter {
    window: Vec<u64>,
    time: i64, // current slot index
    pub window_size: usize,
    pub slot_time: u64, // ms
}

impl RateMeter {
    pub fn new(window_size: usize, slot_time: u64) -> Self {
        let buf = vec![0u64; window_size + 1];
        RateMeter { window: buf, time: 0, window_size, slot_time }
    }

    fn to_time(&self, now_ms: u64) -> i64 {
        let t = now_ms.div_ceil(self.slot_time) as i64;
        t.max(self.time)
    }

    pub fn inc(&mut self, count: u64, now_ms: u64) {
        let now = self.to_time(now_ms);
        let win_len = self.window.len() as i64;
        let cutoff = now - win_len;

        if self.time > cutoff {
            let mut c = cutoff;
            while c > self.time - win_len {
                self.window[(c.rem_euclid(win_len)) as usize] = 0;
                c -= 1;
            }
        } else {
            self.window.fill(0);
        }

        self.window[(now.rem_euclid(win_len)) as usize] += count;
        self.time = now;
    }

    pub fn get_rate(&self, now_ms: u64) -> f64 {
        let now = self.to_time(now_ms);
        let win_len = self.window.len() as i64;
        let cutoff = now - win_len;
        let mut time = self.time;
        let mut rate: u64 = 0;

        while time > cutoff {
            rate += self.window[(time.rem_euclid(win_len)) as usize];
            time -= 1;
        }

        1000.0 * rate as f64 / (self.window_size as f64 * self.slot_time as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> u64 {
        // Use a fixed large value so assert(time > window.length) holds
        10_000_000u64
    }

    #[test]
    fn empty_rate_is_zero() {
        let m = RateMeter::new(10, 100);
        assert_eq!(m.get_rate(now()), 0.0);
    }

    #[test]
    fn single_inc_rate() {
        let mut m = RateMeter::new(10, 100);
        let t = now();
        m.inc(10, t);
        // rate = 1000 * 10 / (10 * 100) = 10.0 rps
        assert!((m.get_rate(t) - 10.0).abs() < 1e-9);
    }

    #[test]
    fn rate_slides_out() {
        let mut m = RateMeter::new(10, 100);
        let t = now();
        m.inc(100, t);
        // After window duration, old counts expire
        let later = t + 10 * 100 + 100; // one full window + one slot later
        assert_eq!(m.get_rate(later), 0.0);
    }

    #[test]
    fn rate_accumulates() {
        let mut m = RateMeter::new(10, 100);
        let t = now();
        // spread 5 items across 5 consecutive slots
        for i in 0..5u64 {
            m.inc(2, t + i * 100);
        }
        // total 10 in window of 1000ms = 10 rps
        assert!((m.get_rate(t + 499) - 10.0).abs() < 1e-9);
    }
}
