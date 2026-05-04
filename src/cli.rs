use camino::Utf8PathBuf;
use clap::Parser;

use crate::encoder::Codec;

/// Streaming MCAP transcoder. Auto-detects color image topics and re-encodes
/// them to H.264 (or H.265) inside `foxglove.CompressedVideo`. Non-color topics
/// and any non-JPEG image topics (e.g. depth PNG) pass through byte-for-byte.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Input MCAP file.
    #[arg(short, long)]
    pub input: Utf8PathBuf,

    /// Output MCAP file. Overwrites if it exists.
    #[arg(short, long)]
    pub output: Utf8PathBuf,

    /// Output video codec. h265 (HEVC) typically gives 30-50 % smaller files
    /// than h264 at the same quality, at the cost of slower encode and a newer
    /// decoder requirement on the consumer side. Both are wire-compatible with
    /// Foxglove Studio's `foxglove.CompressedVideo` renderer.
    #[arg(long, value_enum, default_value_t = Codec::H264)]
    pub codec: Codec,

    /// Encoder CRF (18 = visually transparent, larger files; 20 = default, near-transparent with
    /// smaller files; 23 ≈ ~35 % smaller still but JPEG-era artifacts start to return). Note that
    /// libx265's CRF scale is shifted ~5-6 lower than libx264 — at the same `--crf` value, h265
    /// will be both higher quality and bigger than the equivalent h264. Bump CRF when switching to
    /// h265 if matching file size matters more than matching quality.
    #[arg(long, default_value_t = 20)]
    pub crf: u32,

    /// Encoder preset. Slower = smaller files. Same name space for both x264 and x265
    /// (`ultrafast`, `superfast`, `veryfast`, `faster`, `fast`, `medium`, `slow`, `slower`,
    /// `veryslow`, `placebo`).
    #[arg(long, default_value = "medium")]
    pub preset: String,

    /// Target keyframe interval in seconds (wall-clock via log_time). IDRs are forced manually
    /// so this works for variable-frame-rate streams.
    #[arg(long, default_value_t = 5.0)]
    pub keyframe_interval_seconds: f64,

    /// Maximum consecutive B-frames the encoder may insert between anchors. Higher values
    /// give better compression at the cost of a slightly larger reorder window and a
    /// small encode-speed hit. 0 disables B-frames entirely.
    #[arg(long, default_value_t = 3)]
    pub bframes: u32,

    /// Stop after this many seconds of input (by log_time). Useful for dev iteration.
    #[arg(long)]
    pub limit_seconds: Option<f64>,

    /// Print a progress line every N messages.
    #[arg(long, default_value_t = 2000)]
    pub progress_every: usize,

    /// Extra topics to force into the "color" category even if auto-detect would skip them.
    /// Repeatable.
    #[arg(long)]
    pub force_color: Vec<String>,

    /// Topics to skip classification on (always passthrough). Repeatable.
    #[arg(long)]
    pub skip: Vec<String>,
}
