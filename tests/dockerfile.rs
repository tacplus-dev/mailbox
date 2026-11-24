//! The release image's base is pinned by digest, never a floating tag (#25; tacplus-dev/roadmap
//! decisions/security-gates-ship-only-2026-11-23.md, point 2): a rebuild of the same commit ships the same base.

use std::collections::HashMap;
use std::path::Path;

/// Each `FROM` image of `dockerfile` that is not pinned by digest: its `${ARG}`s resolved to their
/// defaults, an earlier stage's name skipped (it is pinned where it is built).
fn unpinned(dockerfile: &str) -> Vec<String> {
    let mut args = HashMap::new();
    let mut stages = Vec::new();
    let mut found = Vec::new();
    for line in dockerfile.lines().map(str::trim) {
        let mut words = line.split_whitespace();
        match words.next().map(str::to_ascii_uppercase).as_deref() {
            Some("ARG") => {
                for arg in words {
                    if let Some((name, value)) = arg.split_once('=') {
                        args.insert(name.to_string(), value.trim_matches('"').to_string());
                    }
                }
            }
            Some("FROM") => {
                let mut words = words.skip_while(|w| w.starts_with("--"));
                let image = words.next().unwrap_or_default();
                let image = args.iter().fold(image.to_string(), |image, (name, value)| {
                    image
                        .replace(&format!("${{{name}}}"), value)
                        .replace(&format!("${name}"), value)
                });
                if !image.contains("@sha256:") && !stages.contains(&image) {
                    found.push(image);
                }
                if let (Some(as_), Some(stage)) = (words.next(), words.next()) {
                    if as_.eq_ignore_ascii_case("as") {
                        stages.push(stage.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    found
}

#[test]
fn every_from_is_pinned_by_digest() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut checked = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !(name == "Dockerfile" || name.starts_with("Dockerfile.") || name.ends_with(".Dockerfile")) {
            continue;
        }
        let found = unpinned(&std::fs::read_to_string(&path).unwrap());
        assert!(found.is_empty(), "{name}: FROM without @sha256: {found:?}");
        checked += 1;
    }
    assert!(checked > 0, "no Dockerfile found at {}", root.display());
}

#[test]
fn a_floating_tag_is_caught() {
    assert_eq!(unpinned("FROM gcr.io/distroless/cc-debian13\n"), ["gcr.io/distroless/cc-debian13"]);
    assert_eq!(unpinned("from --platform=$BUILDPLATFORM debian:trixie AS build\n"), ["debian:trixie"]);
    assert_eq!(
        unpinned("ARG BASE_IMAGE=gcr.io/distroless/cc-debian13:latest\nFROM ${BASE_IMAGE}\n"),
        ["gcr.io/distroless/cc-debian13:latest"]
    );
}

#[test]
fn a_digest_passes() {
    let digest = "sha256:dac90a25ffe03ebaace90cda886b081e61ab6cd77ac47eda51f7bcc15c320765";
    assert!(unpinned(&format!("FROM gcr.io/distroless/cc-debian13:latest@{digest}\n")).is_empty());
    assert!(unpinned(&format!("ARG BASE_IMAGE=x:1@{digest}\nFROM ${{BASE_IMAGE}}\n")).is_empty());
    assert!(unpinned(&format!("FROM rust:1@{digest} AS build\nFROM build\n")).is_empty());
}
