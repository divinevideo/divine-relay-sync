// ABOUTME: Tests for date string parsing
// ABOUTME: Verifies YYYY-MM-DD and relative date formats

use relay_sync::cli::parse_date;

#[test]
fn test_parse_date_ymd() {
    let ts = parse_date("2024-01-15").unwrap();
    // 2024-01-15 00:00:00 UTC
    assert_eq!(ts, 1705276800);
}

#[test]
fn test_parse_date_ymd_hms() {
    let ts = parse_date("2024-01-15T12:30:00").unwrap();
    assert_eq!(ts, 1705321800);
}

#[test]
fn test_parse_date_timestamp() {
    let ts = parse_date("1705276800").unwrap();
    assert_eq!(ts, 1705276800);
}

#[test]
fn test_parse_date_invalid() {
    assert!(parse_date("not-a-date").is_err());
}
