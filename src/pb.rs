//! prost-generated protobuf types and the FileDescriptorSet we embed as
//! the MCAP schema for `foxglove.CompressedVideo`.
//!
//! Both `pb_gen/foxglove.rs` and `pb_gen/descriptor_set.bin` are vendored from
//! `prost-build` output. Regenerating requires `protoc` and `prost-build`, but
//! end-users installing slimcap via `cargo install` do not — the files are
//! checked in. If you change a `.proto` under `proto/`, regenerate with
//! `tools/regen_proto.sh` and commit the updated files.

pub mod foxglove {
    include!("pb_gen/foxglove.rs");
}

/// FileDescriptorSet bytes for `foxglove.CompressedVideo` and its transitive
/// dependency `google.protobuf.Timestamp`. Stored as the `schema.data` for the
/// output MCAP's `foxglove.CompressedVideo` schema record so downstream tools
/// (Foxglove Studio, Rerun) can fully resolve the type.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("pb_gen/descriptor_set.bin");
