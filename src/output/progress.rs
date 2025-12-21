// ABOUTME: Progress reporting with indicatif
// ABOUTME: Shows sync progress with event counts and rates

use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub struct ProgressReporter {
    bar: ProgressBar,
    quiet: bool,
}

impl ProgressReporter {
    pub fn new(total: Option<u64>, quiet: bool) -> Self {
        let bar = if let Some(total) = total {
            ProgressBar::new(total)
        } else {
            ProgressBar::new_spinner()
        };

        if !quiet {
            if total.is_some() {
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {per_sec} | eta: {eta}")
                        .unwrap()
                        .progress_chars("#>-"),
                );
            } else {
                bar.set_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner:.green} {pos} events | {per_sec}")
                        .unwrap(),
                );
            }
            bar.enable_steady_tick(Duration::from_millis(100));
        }

        Self { bar, quiet }
    }

    pub fn set_message(&self, msg: impl Into<String>) {
        if !self.quiet {
            self.bar.set_message(msg.into());
        }
    }

    pub fn inc(&self, n: u64) {
        self.bar.inc(n);
    }

    pub fn finish(&self, msg: impl Into<String>) {
        if !self.quiet {
            self.bar.finish_with_message(msg.into());
        }
    }

    pub fn println(&self, msg: impl AsRef<str>) {
        if !self.quiet {
            self.bar.println(msg.as_ref());
        }
    }
}
