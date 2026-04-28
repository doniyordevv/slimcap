#!/usr/bin/env bash
# Regenerate src/pb_gen/{foxglove.rs,descriptor_set.bin} from proto/.
#
# Only needed when a .proto file changes — end users installing slimcap
# never run this.
#
# Requires: protoc on PATH, plus a working Rust toolchain (we drive
# prost-build through a tiny throwaway script so we don't have to keep
# prost-build in the published crate's build dependencies).
set -euo pipefail

cd "$(dirname "$0")/.."

cat > /tmp/slimcap-regen-proto/Cargo.toml <<'TOML' 2>/dev/null || true
mkdir -p /tmp/slimcap-regen-proto/src
cat > /tmp/slimcap-regen-proto/Cargo.toml <<'TOML'
[package]
name = "slimcap-regen-proto"
version = "0.0.0"
edition = "2021"
[dependencies]
prost-build = "0.13"
TOML

cat > /tmp/slimcap-regen-proto/src/main.rs <<RUST
fn main() -> std::io::Result<()> {
    let target = std::env::args().nth(1).expect("target dir");
    std::fs::create_dir_all(&target)?;
    let mut cfg = prost_build::Config::new();
    cfg.out_dir(&target);
    cfg.file_descriptor_set_path(format!("{}/descriptor_set.bin", target));
    cfg.extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp");
    cfg.compile_protos(
        &[
            "$(pwd)/proto/foxglove/CompressedVideo.proto",
            "$(pwd)/proto/google/protobuf/timestamp.proto",
        ],
        &["$(pwd)/proto"],
    )?;
    Ok(())
}
RUST

cargo run --manifest-path /tmp/slimcap-regen-proto/Cargo.toml -- "$(pwd)/src/pb_gen"
echo "regenerated src/pb_gen/{foxglove.rs,descriptor_set.bin}"
