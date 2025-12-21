// ABOUTME: Adaptive rate limiter using governor crate
// ABOUTME: Adjusts rate based on relay responses (429s, rate-limited errors)

use governor::{Quota, RateLimiter as GovLimiter};
use nonzero_ext::nonzero;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};

/// Adaptive rate limiter that adjusts based on relay feedback
pub struct RateLimiter {
    limiter: GovLimiter<governor::state::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>,
    current_rate: AtomicU32,
    min_rate: u32,
    max_rate: u32,
}

impl RateLimiter {
    /// Create new rate limiter with initial rate (events per second)
    pub fn new(initial_rate: u32) -> Self {
        let rate = NonZeroU32::new(initial_rate).unwrap_or(nonzero!(10u32));
        let quota = Quota::per_second(rate);
        let limiter = GovLimiter::direct(quota);

        Self {
            limiter,
            current_rate: AtomicU32::new(initial_rate),
            min_rate: 1,
            max_rate: 100,
        }
    }

    /// Wait for permission to send
    pub async fn wait(&self) {
        self.limiter.until_ready().await;
    }

    /// Record a rate limit response - slow down
    pub fn record_rate_limited(&self) {
        let current = self.current_rate.load(Ordering::Relaxed);
        let new_rate = (current / 2).max(self.min_rate);
        self.current_rate.store(new_rate, Ordering::Relaxed);
        tracing::warn!("Rate limited, reducing to {} events/sec", new_rate);
    }

    /// Record successful publish - can speed up
    pub fn record_success(&self) {
        let current = self.current_rate.load(Ordering::Relaxed);
        if current < self.max_rate {
            let new_rate = (current + 1).min(self.max_rate);
            self.current_rate.store(new_rate, Ordering::Relaxed);
        }
    }

    /// Get current rate
    pub fn current_rate(&self) -> u32 {
        self.current_rate.load(Ordering::Relaxed)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(20) // 20 events/sec default
    }
}
