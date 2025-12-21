// ABOUTME: Failure log entry parsing
// ABOUTME: Tracks events that failed to sync for retry

#[derive(Debug, Clone)]
pub struct FailureEntry {
    pub timestamp: i64,
    pub event_id: String,
    pub reason: String,
}

impl FailureEntry {
    pub fn parse(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() >= 3 {
            Some(Self {
                timestamp: parts[0].parse().ok()?,
                event_id: parts[1].to_string(),
                reason: parts[2].to_string(),
            })
        } else {
            None
        }
    }
}
