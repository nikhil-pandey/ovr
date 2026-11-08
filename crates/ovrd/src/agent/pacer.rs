//! Pacing for bulk transfers that share a connection with interactive runs.
//!
//! Everything a host sends waits in the same queues once it leaves `ovrd`: the socket, SSH, a tunnel's forwarder.
//! A bulk stream that fills them delays every other stream's frames by as long as the queue takes to drain. The
//! pacer keeps no more than about twice the link's bandwidth-delay product unacknowledged: enough to fill the link,
//! doubling each round trip while the link has room, and leaving at most about one round trip queued in front of it. The lowest round trip seen is the link itself; anything above it is queueing.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Never less in flight than this, so that a fast local link is not throttled by measurement noise.
const FLOOR: u64 = 256 * 1024;
/// In flight relative to the bandwidth-delay product: above one, so the budget grows while the link has room.
const GAIN: f64 = 2.0;
/// Delivery rates are the best seen over this many round trips, and at least a second.
const RATE_WINDOW: u32 = 10;

#[derive(Debug, Default)]
pub struct Pacer {
    sent: u64,
    acked: u64,
    /// Sent and not yet acknowledged: where each send ended, when, and how much was acknowledged then.
    flight: VecDeque<(u64, Instant, u64)>,
    min_rtt: Option<Duration>,
    /// Recent delivery rates in bytes per second, oldest first.
    rates: VecDeque<(Instant, f64)>,
}

impl Pacer {
    /// Whether more may be sent now.
    #[must_use]
    pub fn has_room(&self) -> bool {
        self.sent - self.acked < self.budget()
    }

    /// How much may be unacknowledged.
    #[must_use]
    pub fn budget(&self) -> u64 {
        let rate = self.rates.iter().map(|&(_, rate)| rate).fold(0.0, f64::max);
        match self.min_rtt {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Some(rtt) => FLOOR.max((rate * rtt.as_secs_f64() * GAIN) as u64),
            None => FLOOR,
        }
    }

    /// Records `bytes` sent at `now`.
    pub fn sent(&mut self, bytes: u64, now: Instant) {
        self.sent += bytes;
        self.flight.push_back((self.sent, now, self.acked));
    }

    /// Records that the receiver has `total` bytes, as of `now`.
    pub fn acked(&mut self, total: u64, now: Instant) {
        let total = total.min(self.sent);
        if total <= self.acked {
            return;
        }
        self.acked = total;
        while let Some(&(end, at, acked_then)) = self.flight.front() {
            if end > total {
                break;
            }
            self.flight.pop_front();
            let rtt = now
                .saturating_duration_since(at)
                .max(Duration::from_micros(1));
            self.min_rtt = Some(self.min_rtt.map_or(rtt, |min| min.min(rtt)));
            #[allow(clippy::cast_precision_loss)]
            let rate = (total - acked_then) as f64 / rtt.as_secs_f64();
            self.rates.push_back((now, rate));
        }
        let window = self
            .min_rtt
            .map_or(Duration::ZERO, |rtt| rtt * RATE_WINDOW)
            .max(Duration::from_secs(1));
        while self
            .rates
            .front()
            .is_some_and(|&(at, _)| now.saturating_duration_since(at) > window)
        {
            self.rates.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Sends 64 KiB frames through a link of `rate` bytes per second and round trip `rtt` for `seconds`, acknowledging
    /// each frame a round trip after it leaves the link. Returns the throughput and the longest any frame queued.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn simulate(rate: f64, rtt: Duration, seconds: u64) -> (f64, Duration) {
        const FRAME: u64 = 64 * 1024;
        let start = Instant::now();
        let mut pacer = Pacer::default();
        let mut link_free = start;
        let mut acks = VecDeque::new();
        let mut queued = Duration::ZERO;
        let mut sent = 0;
        let step = Duration::from_millis(1);
        let mut now = start;
        while now < start + Duration::from_secs(seconds) {
            while acks.front().is_some_and(|&(at, _)| at <= now) {
                if let Some((_, total)) = acks.pop_front() {
                    pacer.acked(total, now);
                }
            }
            while pacer.has_room() {
                let begin = link_free.max(now);
                if now > start + Duration::from_secs(1) {
                    queued = queued.max(begin - now);
                }
                link_free = begin + Duration::from_secs_f64(FRAME as f64 / rate);
                sent += FRAME;
                pacer.sent(FRAME, now);
                acks.push_back((link_free + rtt, sent));
            }
            now += step;
        }
        (pacer.acked as f64 / seconds as f64, queued)
    }

    #[test]
    fn fills_a_slow_distant_link_without_queueing_much() {
        let rtt = Duration::from_millis(80);
        let rate = 1.25e6; // 10 Mbit/s
        let (throughput, queued) = simulate(rate, rtt, 20);
        assert!(throughput > rate * 0.9, "{throughput} of {rate}");
        // One frame alone takes 52 ms at this rate, so the round trip the pacer sees is about 130 ms.
        assert!(queued < Duration::from_millis(200), "queued for {queued:?}");
    }

    #[test]
    fn fills_a_fast_link() {
        let rtt = Duration::from_millis(20);
        let rate = 125e6; // 1 Gbit/s
        let (throughput, queued) = simulate(rate, rtt, 10);
        assert!(throughput > rate * 0.9, "{throughput} of {rate}");
        assert!(queued < rtt * 3 / 2, "queued for {queued:?}");
    }

    #[test]
    fn starts_at_the_floor_and_ignores_stale_acknowledgements() {
        let now = Instant::now();
        let mut pacer = Pacer::default();
        assert_eq!(pacer.budget(), FLOOR);
        pacer.sent(FLOOR, now);
        assert!(!pacer.has_room());
        pacer.acked(FLOOR * 4, now + Duration::from_millis(10));
        assert!(pacer.has_room());
        pacer.acked(0, now + Duration::from_millis(20));
        assert_eq!(pacer.acked, FLOOR);
    }
}
