# slimcap

**slimcap compresses MCAP files to make them smaller.**

It walks an input MCAP, auto-detects color image topics (JPEG inside
`sensor_msgs/CompressedImage` or `foxglove.CompressedImage`), and re-encodes
them to H.264 (or H.265) inside `foxglove.CompressedVideo`. Everything else
— IMU, TF, odom, depth, camera_info, custom messages — is passed through
byte-for-byte.

## Install

```bash
cargo install slimcap
```

slimcap links against the system FFmpeg (libavcodec/libavformat/libavutil/
libswscale) and libx264. libx265 is optional — only needed if you pass
`--codec h265`. Install dependencies before `cargo install`.

**Ubuntu / Debian:**

```bash
sudo apt-get install \
  libavcodec-dev libavformat-dev libavutil-dev libswscale-dev \
  libx264-dev libx265-dev pkg-config
```

(Drop `libx265-dev` if you only need h264.)

**macOS (Homebrew):**

```bash
brew install ffmpeg pkg-config
```

(Homebrew's `ffmpeg` bottle ships with both x264 and x265 enabled.)

`protoc` is **not** required — the compiled protobuf descriptors are vendored
in the published crate.

## Usage

```bash
slimcap --input INPUT.mcap --output OUTPUT.mcap
```

Common options:

| Flag | Default | Effect |
|---|---|---|
| `--codec <NAME>` | `h264` | Output video codec. `h265` (HEVC) typically gives 30-50 % smaller files at the same quality, in exchange for slower encode and a newer decoder requirement on the consumer side. Both ride inside `foxglove.CompressedVideo`. |
| `--crf <N>` | 20 | Encoder CRF. 18 = visually transparent. 23 ≈ 35 % smaller files but JPEG-era artifacts return. libx265's CRF scale is shifted ~5-6 lower than libx264 — bump CRF when switching to h265 if you want comparable file sizes rather than comparable quality. |
| `--preset <NAME>` | `medium` | Encoder preset. `slow` for ~10 % smaller output, much slower encode. Same name space for x264 and x265. |
| `--keyframe-interval-seconds <S>` | 5.0 | Wall-clock IDR cadence (uses the message `log_time`, not a frame counter, so VFR streams stay aligned). Longer = smaller files, slower scrubbing. |
| `--bframes <N>` | 3 | Max consecutive B-frames between anchors. 0 disables; higher values trade a slightly larger reorder window for better compression. |
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

The encoder is configured with `scenecut=0`, `keyint=9999`, `min-keyint=1`
(plus `no-open-gop=1` on libx265). IDR keyframes are forced manually whenever
`log_time - last_idr_log_time ≥ keyframe_interval_seconds`, so GOP duration
stays bounded against the bag's wall-clock regardless of capture jitter.
Per-frame `pts` is the message's ROS log time in microseconds offset from the
topic's first frame, so the encoded stream preserves the original capture
spacing exactly.

## Output schema

Color topics are re-emitted on the same topic name with:

- Schema: `foxglove.CompressedVideo` (protobuf), wire-compatible with
  Foxglove Studio's built-in renderer.
- Channel metadata gains `transcoded_from = "<input schema name>"` for
  provenance.
- `format = "h264"` or `"h265"` (per `--codec`), Annex-B NAL units, parameter
  sets (SPS+PPS for h264, VPS+SPS+PPS for h265) inline on every IDR for
  single-message random access.

## License

Source code is dual-licensed under [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.

**Note on linked libraries:** slimcap links to libx264 and (when built with
H.265 support) libx265. Both are GPL-2.0+. Binary distributions of slimcap
therefore inherit GPL terms. The Rust source crate itself remains MIT/Apache-2.0;
the GPL constraint applies only to compiled binaries that include libx264/libx265.
If you redistribute a compiled slimcap, comply with the linked libraries'
GPL accordingly. Building locally with `cargo install` for your own use is not
"distribution" under the GPL.
