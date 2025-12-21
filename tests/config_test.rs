// ABOUTME: Tests for TOML config file parsing
// ABOUTME: Verifies sync config and auth settings

use relay_sync::config::Config;

#[test]
fn test_parse_config() {
    let toml = r#"
[auth]
nsec = "nsec1test"

[[sync]]
name = "test-sync"
source = "relay.source.com"
dest = "relay.dest.com"
kinds = [1, 7]
"#;

    let config = Config::from_str(toml).unwrap();
    assert_eq!(config.auth.as_ref().unwrap().nsec, Some("nsec1test".to_string()));
    assert_eq!(config.sync.len(), 1);
    assert_eq!(config.sync[0].name, "test-sync");
    assert_eq!(config.sync[0].kinds, Some(vec![1, 7]));
}

#[test]
fn test_find_sync_by_name() {
    let toml = r#"
[[sync]]
name = "first"
source = "a.com"
dest = "b.com"

[[sync]]
name = "second"
source = "c.com"
dest = "d.com"
"#;

    let config = Config::from_str(toml).unwrap();
    let sync = config.find_sync("second").unwrap();
    assert_eq!(sync.source, "c.com");
}

#[test]
fn test_env_var_substitution() {
    std::env::set_var("TEST_NSEC", "nsec1fromenv");

    let toml = r#"
[auth]
nsec = "${TEST_NSEC}"
"#;

    let config = Config::from_str(toml).unwrap();
    assert_eq!(
        config.auth.as_ref().unwrap().resolve_nsec().unwrap(),
        "nsec1fromenv"
    );

    std::env::remove_var("TEST_NSEC");
}
