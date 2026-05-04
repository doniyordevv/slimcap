//! Per-topic JPEG→H.264/H.265 re-encoder with variable-frame-rate pts.
//!
//! Each [`TopicEncoder`] holds its own MJPEG decoder, swscale context, and
//! libx264/libx265 encoder. The video encoders keep per-stream reference
//! pictures, so frames from different cameras must not share an encoder.
//!
//! Timing model:
//!
//! - Encoder `time_base` is 1 µs. `pts` for each frame is `log_time_ns / 1000`
//!   offset to zero on the first frame of the stream. That gives VFR: pts
//!   reflects the actual capture/receive spacing, not a nominal 30 Hz grid.
//! - Scenecut is disabled and `keyint` is set to a huge value; we force IDRs
//!   manually whenever `log_time - last_idr_log_time >= keyframe_interval`.
//!   That guarantees GOP boundaries fall at fixed wall-clock intervals
//!   regardless of frame rate jitter.
//! - With B-frames enabled, libx264/libx265 emit packets in decode order,
//!   not display order. We hold the most recent `bframes` packets in a
//!   per-topic `BTreeMap` keyed on PTS and release the smallest first, so
//!   MCAP messages on each topic stay monotonic in `log_time` (what indexers
//!   and `MessageStream` expect).

use std::collections::{BTreeMap, HashMap};

use anyhow::{anyhow, bail, Context as _, Result};
use ffmpeg_next as ff;
use ff::codec::Id;
use ff::format::Pixel;
use ff::software::scaling::{Context as Scaler, Flags};
use ff::util::picture;
use ff::util::rational::Rational;

const TIME_BASE_NUM: i32 = 1;
const TIME_BASE_DEN: i32 = 1_000_000; // 1 µs

/// Output video codec. Determines the libavcodec encoder name, the
/// codec-specific params string, and the `format` field on the emitted
/// `foxglove.CompressedVideo` message.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Codec {
    #[clap(name = "h264")]
    H264,
    #[clap(name = "h265")]
    H265,
}

impl Codec {
    pub fn ffmpeg_encoder_name(self) -> &'static str {
        match self {
            Codec::H264 => "libx264",
            Codec::H265 => "libx265",
        }
    }

    /// Value for the `format` field on `foxglove.CompressedVideo`.
    pub fn foxglove_format(self) -> &'static str {
        match self {
            Codec::H264 => "h264",
            Codec::H265 => "h265",
        }
    }

    fn x_params_key(self) -> &'static str {
        match self {
            Codec::H264 => "x264-params",
            Codec::H265 => "x265-params",
        }
    }

    /// Codec-specific options bundle. Both strings disable the encoder's own
    /// keyframe cadence (we force IDRs manually from `log_time`) and ensure
    /// each IDR is a self-contained random-access point with parameter sets
    /// inline — required for Foxglove's single-message playback model.
    fn x_params_value(self) -> &'static str {
        match self {
            Codec::H264 => "repeat-headers=1:annexb=1:keyint=9999:min-keyint=1:scenecut=0",
            // libx265 emits Annex-B by default through libavcodec (no annexb opt).
            // no-open-gop=1 closes the GOP at every IDR so B-frames never reach
            // back across the boundary.
            Codec::H265 => "repeat-headers=1:keyint=9999:min-keyint=1:scenecut=0:no-open-gop=1",
        }
    }
}

impl std::fmt::Display for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.foxglove_format())
    }
}

/// Metadata we need to emit one output message, keyed on the pts we handed to
/// the encoder. The map is needed because packets may come back several calls
/// later than the frame we pushed (lookahead + B-frame reordering).
#[derive(Debug, Clone)]
pub struct FrameMeta {
    pub log_time: u64,
    pub publish_time: u64,
    pub sequence: u32,
    pub stamp_sec: i64,
    pub stamp_nanos: i32,
    pub frame_id: String,
}

/// One packet ready to be written to the output MCAP.
#[derive(Debug)]
pub struct EncodedPacket {
    pub meta: FrameMeta,
    pub data: Vec<u8>,
}

/// Per-topic decoder + encoder pipeline. Initialised lazily on the first frame
/// because we need to know the image dimensions before building the encoder.
pub struct TopicEncoder {
    jpeg_decoder: ff::codec::decoder::Video,
    encoder: Option<EncoderState>,
    codec: Codec,
    crf: u32,
    preset: String,
    bframes: u32,
    keyframe_interval_ns: i64,
    pending: HashMap<i64, FrameMeta>,
    /// Output reorder buffer. libx264 hands back packets in decode order, but
    /// we want to write the MCAP per-topic in display/PTS order so log_time is
    /// monotonic. Holds the most recent `bframes` packets in steady state.
    reorder: BTreeMap<i64, EncodedPacket>,

    start_log_ns: Option<u64>,
    last_idr_log_ns: Option<u64>,
    last_pts_us: Option<i64>,
}

struct EncoderState {
    enc: ff::codec::encoder::Video,
    width: u32,
    height: u32,
    /// Cached scaler; rebuilt whenever the decoded source format changes,
    /// which happens for some JPEG producers that mix subsampling modes
    /// mid-stream (e.g. yuvj422p vs yuvj420p frames).
    scaler: Option<(Pixel, Scaler)>,
}

impl TopicEncoder {
    pub fn new(
        codec: Codec,
        crf: u32,
        preset: &str,
        keyframe_interval_seconds: f64,
        bframes: u32,
    ) -> Result<Self> {
        let dec_codec = ff::codec::decoder::find(Id::MJPEG)
            .ok_or_else(|| anyhow!("libavcodec has no MJPEG decoder"))?;
        let dec_ctx = ff::codec::context::Context::new_with_codec(dec_codec);
        let jpeg_decoder = dec_ctx
            .decoder()
            .video()
            .context("open MJPEG decoder")?;

        let keyframe_interval_ns = (keyframe_interval_seconds * 1_000_000_000.0) as i64;

        Ok(Self {
            jpeg_decoder,
            encoder: None,
            codec,
            crf,
            preset: preset.to_string(),
            bframes,
            keyframe_interval_ns,
            pending: HashMap::new(),
            reorder: BTreeMap::new(),
            start_log_ns: None,
            last_idr_log_ns: None,
            last_pts_us: None,
        })
    }

    /// Push one JPEG frame. Returns any output packets the encoder is able to
    /// emit as a result (usually one, occasionally zero while the encoder is
    /// warming up, never more than a few).
    pub fn push_frame(
        &mut self,
        jpeg_bytes: &[u8],
        meta: FrameMeta,
    ) -> Result<Vec<EncodedPacket>> {
        // 1. Decode the JPEG.
        let decoded = self.decode_jpeg(jpeg_bytes)?;

        // 2. Build the encoder if this is the first frame (or reinit if the
        //    dimensions changed — we bail because that is almost always a
        //    data bug and silently carrying on would just corrupt the stream).
        let (w, h, src_fmt) = (
            decoded.width(),
            decoded.height(),
            decoded.format(),
        );
        if self.encoder.is_none() {
            self.encoder = Some(self.init_encoder(w, h, src_fmt)?);
        } else {
            let state = self.encoder.as_ref().unwrap();
            if state.width != w || state.height != h {
                bail!(
                    "resolution changed mid-stream ({}x{} -> {}x{}); cannot continue",
                    state.width,
                    state.height,
                    w,
                    h
                );
            }
        }

        // 3. Compute pts / force IDR / encode.
        let first_frame = self.start_log_ns.is_none();
        if first_frame {
            self.start_log_ns = Some(meta.log_time);
        }
        let start_ns = self.start_log_ns.unwrap();
        // Saturating because log_time is u64 and we might see a rare non-monotonic bump;
        // we also enforce strict monotonicity below.
        let raw_pts_us = ((meta.log_time.saturating_sub(start_ns)) / 1000) as i64;
        let pts_us = match self.last_pts_us {
            Some(last) if raw_pts_us <= last => last + 1,
            _ => raw_pts_us,
        };
        self.last_pts_us = Some(pts_us);

        let force_idr = match self.last_idr_log_ns {
            None => true, // first frame
            Some(last) => {
                meta.log_time.saturating_sub(last) as i64 >= self.keyframe_interval_ns
            }
        };
        if force_idr {
            self.last_idr_log_ns = Some(meta.log_time);
        }

        self.pending.insert(pts_us, meta);

        // 4. Convert to yuv420p and push to the encoder.
        let state = self.encoder.as_mut().unwrap();
        let scaler_mismatch = state
            .scaler
            .as_ref()
            .map(|(fmt, _)| *fmt != src_fmt)
            .unwrap_or(true);
        if scaler_mismatch {
            let scaler = Scaler::get(
                src_fmt,
                state.width,
                state.height,
                Pixel::YUV420P,
                state.width,
                state.height,
                Flags::BILINEAR,
            )
            .context("build swscale context")?;
            state.scaler = Some((src_fmt, scaler));
        }

        let mut yuv = ff::frame::Video::new(Pixel::YUV420P, state.width, state.height);
        let (_, scaler) = state.scaler.as_mut().unwrap();
        scaler
            .run(&decoded, &mut yuv)
            .context("swscale yuv -> yuv420p")?;
        yuv.set_pts(Some(pts_us));
        if force_idr {
            yuv.set_kind(picture::Type::I);
        }

        state.enc.send_frame(&yuv).context("send_frame")?;

        self.drain(false)
    }

    /// Flush the encoder. Must be called once per topic after the last frame
    /// so the final packets are emitted.
    pub fn finish(&mut self) -> Result<Vec<EncodedPacket>> {
        if let Some(state) = self.encoder.as_mut() {
            state.enc.send_eof().context("send_eof")?;
        }
        self.drain(true)
    }

    fn drain(&mut self, flush: bool) -> Result<Vec<EncodedPacket>> {
        let Some(state) = self.encoder.as_mut() else {
            return Ok(Vec::new());
        };
        loop {
            let mut packet = ff::codec::packet::Packet::empty();
            match state.enc.receive_packet(&mut packet) {
                Ok(()) => {
                    let pts = packet.pts().unwrap_or(0);
                    let meta = self
                        .pending
                        .remove(&pts)
                        .ok_or_else(|| anyhow!("no pending meta for pts {}", pts))?;
                    let data = packet.data().unwrap_or_default().to_vec();
                    self.reorder.insert(pts, EncodedPacket { meta, data });
                }
                Err(ff::Error::Other { errno })
                    if errno == ff::error::EAGAIN.into() =>
                {
                    break;
                }
                Err(ff::Error::Eof) => break,
                Err(e) => return Err(e).context("receive_packet"),
            }
        }

        // libx264's PTS-vs-DTS reorder window is bounded by `bframes`. Once
        // the buffer holds more than that, the smallest-PTS entry is committed:
        // no future packet can carry a smaller PTS. On flush we release
        // everything since no more frames will arrive.
        let hold = if flush { 0 } else { self.bframes as usize };
        let mut out = Vec::new();
        while self.reorder.len() > hold {
            let (_, pkt) = self.reorder.pop_first().unwrap();
            out.push(pkt);
        }

        if flush && !self.pending.is_empty() {
            tracing::warn!(
                pending = self.pending.len(),
                "flush completed with unconsumed pending metadata entries"
            );
        }
        Ok(out)
    }

    fn decode_jpeg(&mut self, jpeg_bytes: &[u8]) -> Result<ff::frame::Video> {
        let mut packet = ff::codec::packet::Packet::copy(jpeg_bytes);
        packet.set_pts(None);
        packet.set_dts(None);
        self.jpeg_decoder
            .send_packet(&packet)
            .context("jpeg send_packet")?;
        let mut frame = ff::frame::Video::empty();
        match self.jpeg_decoder.receive_frame(&mut frame) {
            Ok(()) => Ok(frame),
            Err(e) => Err(anyhow!("mjpeg decode: {}", e)),
        }
    }

    fn init_encoder(&self, w: u32, h: u32, _src_fmt: Pixel) -> Result<EncoderState> {
        if w % 2 != 0 || h % 2 != 0 {
            bail!("yuv420p requires even width and height, got {}x{}", w, h);
        }

        let codec_name = self.codec.ffmpeg_encoder_name();
        let enc_codec = ff::codec::encoder::find_by_name(codec_name)
            .ok_or_else(|| anyhow!("libavcodec has no {codec_name} encoder"))?;

        let enc_ctx = ff::codec::context::Context::new_with_codec(enc_codec);
        let mut enc = enc_ctx.encoder().video()?;
        enc.set_width(w);
        enc.set_height(h);
        enc.set_format(Pixel::YUV420P);
        enc.set_time_base(Rational(TIME_BASE_NUM, TIME_BASE_DEN));
        enc.set_max_b_frames(self.bframes as usize);
        // Nominal framerate — CRF encoding largely ignores this, but some rate-
        // control heuristics read it. 30 is a reasonable prior for all sensor
        // streams we've seen; pts differences give the encoder the actual durations.
        enc.set_frame_rate(Some(Rational(30, 1)));

        let mut opts = ff::Dictionary::new();
        opts.set("preset", &self.preset);
        opts.set("crf", &self.crf.to_string());
        opts.set(self.codec.x_params_key(), self.codec.x_params_value());

        let enc = enc
            .open_with(opts)
            .with_context(|| format!("open {codec_name} encoder"))?;

        Ok(EncoderState {
            enc,
            width: w,
            height: h,
            scaler: None,
        })
    }
}
