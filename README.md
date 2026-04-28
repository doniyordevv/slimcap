# slimcap

**slimcap compresses MCAP files to make them smaller.**

It walks an input MCAP, auto-detects color image topics (JPEG inside
`sensor_msgs/CompressedImage` or `foxglove.CompressedImage`), and re-encodes
them to H.264 inside `foxglove.CompressedVideo`. Everything else — IMU, TF,
odom, depth, camera_info, custom messages — is passed through byte-for-byte.

Typical result on a multi-camera teleop bag: **6.5 GiB → 2.0 GiB (~69 % smaller)**
with no perceptible quality loss (CRF 20, libx264 preset=medium).

## Install

```bash
cargo install slimcap
```

slimcap links against the system FFmpeg (libavcodec/libavformat/libavutil/
libswscale) and libx264. Install those before `cargo install`.

**Ubuntu / Debian:**

```bash
sudo apt-get install \
  libavcodec-dev libavformat-dev libavutil-dev libswscale-dev \
  libx264-dev pkg-config
```

**macOS (Homebrew):**

```bash
brew install ffmpeg pkg-config
```

`protoc` is **not** required — the compiled protobuf descriptors are vendored
in the published crate.

## Usage

```bash
slimcap --input INPUT.mcap --output OUTPUT.mcap
```

Common options:

| Flag | Default | Effect |
|---|---|---|
| `--crf <N>` | 20 | x264 CRF. 18 = visually transparent. 23 ≈ 35 % smaller files but JPEG-era artifacts return. |
| `--preset <NAME>` | `medium` | x264 preset. `slow` for ~10 % smaller output, much slower encode. |
| `--keyframe-interval-seconds <S>` | 2.0 | Wall-clock IDR cadence (uses the message `log_time`, not a frame counter, so VFR streams stay aligned). |
| `--limit-seconds <S>` | — | Stop after N seconds of input. Useful for dev iteration. |
| `--skip <TOPIC>` | — | Force a topic to passthrough even if auto-detection picks it as color. Repeatable. |
| `--force-color <TOPIC>` | — | Force a topic into the color path. Repeatable. |
| `--progress-every <N>` | 2000 | Print a progress line every N input messages. |

## How auto-detection works

A topic is re-encoded to H.264 if and only if **both** are true:

1. The schema name is one of:
   - `sensor_msgs/msg/CompressedImage`
   - `sensor_msgs/CompressedImage`
   - `foxglove.CompressedImage`
2. The first message's image payload starts with the JPEG SOI marker
   (`FF D8 FF`).

PNG payloads (typically depth) are passed through. For protobuf inputs, the
field-number mapping is read from the schema's embedded `FileDescriptorSet`,
so producers that use non-canonical numbering (e.g. some microagi rigs that
swap `data` and `frame_id` field numbers) work correctly.

## Variable frame rate

x264 is configured with `scenecut=0`, `keyint=9999`, `min-keyint=1`. IDR
keyframes are forced manually whenever `log_time - last_idr_log_time ≥
keyframe_interval_seconds`, so GOP duration stays bounded against the bag's
wall-clock regardless of capture jitter. Per-frame `pts` is the message's
ROS log time in microseconds offset from the topic's first frame, so the
encoded stream preserves the original capture spacing exactly.

## Output schema

Color topics are re-emitted on the same topic name with:

- Schema: `foxglove.CompressedVideo` (protobuf), wire-compatible with
  Foxglove Studio's built-in renderer.
- Channel metadata gains `transcoded_from = "<input schema name>"` for
  provenance.
- `format = "h264"`, Annex-B NAL units, SPS+PPS inline on every IDR
  (single-message random access).

## License

Source code is dual-licensed under [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.

**Note on linked libraries:** slimcap links to libx264 (GPL-2.0+). Binary
distributions of slimcap therefore inherit GPL terms. The Rust source crate
itself remains MIT/Apache-2.0; the GPL constraint applies only to compiled
binaries that include libx264. If you redistribute a compiled slimcap, comply
with libx264's GPL accordingly. Building locally with `cargo install` for
your own use is not "distribution" under the GPL.
