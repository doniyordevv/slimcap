use camino::Utf8PathBuf;
use clap::Parser;

/// Streaming MCAP transcoder. Auto-detects color image topics and re-encodes
/// them to H.264 inside `foxglove.CompressedVideo`. Non-color topics and any
/// non-JPEG image topics (e.g. depth PNG) pass through byte-for-byte.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Input MCAP file.
    #[arg(short, long)]
    pub input: Utf8PathBuf,

    /// Output MCAP file. Overwrites if it exists.
    #[arg(short, long)]
    pub output: Utf8PathBuf,

    /// x264 CRF (18 = visually transparent, larger files; 20 = default, near-transparent with
    /// smaller files; 23 = x264's own default, ~35 % smaller still but JPEG-era artifacts start
    /// to return).
    #[arg(long, default_value_t = 20)]
    pub crf: u32,

    /// x264 preset. Slower = smaller files.
    #[arg(long, default_value = "medium")]
    pub preset: String,

    /// Target keyframe interval in seconds (wall-clock via log_time). IDRs are forced manually
    /// so this works for variable-frame-rate streams.
    #[arg(long, default_value_t = 2.0)]
    pub keyframe_interval_seconds: f64,

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
