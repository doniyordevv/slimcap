//! End-to-end streaming pipeline: read input MCAP, classify each channel on
//! first sight, re-encode color topics, passthrough the rest, write a new
//! MCAP with zstd-chunked output.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::time::Instant;

use anyhow::{anyhow, Context as _, Result};
use mcap::records::MessageHeader;
use mcap::{Summary, WriteOptions, Writer};
use prost::Message as _;

use crate::cdr::{self, ImageFields};
use crate::cli::Args;
use crate::detect::{classify_first_message, ImageSchema, TopicKind};
use crate::encoder::{Codec, EncodedPacket, FrameMeta, TopicEncoder};
use crate::ordered::OrderedReader;
use crate::pb;

/// One output message waiting in the reorder buffer.
struct QueuedMsg {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

/// Bounded output reorder buffer.
///
/// Passthrough messages are produced with zero latency, but the encoder emits
/// a frame's packet many frames after it was pushed (lookahead + thread +
/// B-frame latency), so video packets lag behind. Writing in production order
/// would interleave the two streams non-monotonically. Instead every output
/// message is held here keyed by `log_time`; only messages older than
/// `window_ns` behind the newest enqueued one are flushed, so the written MCAP
/// stays monotonic in `log_time` and `mcap doctor`-clean. Peak memory is
/// ~`window` seconds of output.
struct OutBuf {
    buf: BTreeMap<(u64, u64), QueuedMsg>,
    seq: u64,
    window_ns: u64,
    newest_ns: u64,
}

impl OutBuf {
    fn new(window_ns: u64) -> Self {
        Self {
            buf: BTreeMap::new(),
            seq: 0,
            window_ns,
            newest_ns: 0,
        }
    }

    fn enqueue(&mut self, m: QueuedMsg) {
        self.newest_ns = self.newest_ns.max(m.log_time);
        let key = (m.log_time, self.seq);
        self.seq += 1;
        self.buf.insert(key, m);
    }

    fn write<W: Write + Seek>(w: &mut Writer<W>, m: &QueuedMsg) -> Result<()> {
        w.write_to_known_channel(
            &MessageHeader {
                channel_id: m.channel_id,
                sequence: m.sequence,
                log_time: m.log_time,
                publish_time: m.publish_time,
            },
            &m.data,
        )?;
        Ok(())
    }

    /// Flush everything more than `window_ns` behind the newest message.
    fn flush_ready<W: Write + Seek>(&mut self, w: &mut Writer<W>) -> Result<()> {
        let cutoff = self.newest_ns.saturating_sub(self.window_ns);
        while let Some((&(lt, _), _)) = self.buf.first_key_value() {
            if lt > cutoff {
                break;
            }
            let (_, m) = self.buf.pop_first().unwrap();
            Self::write(w, &m)?;
        }
        Ok(())
    }

    /// Drain the buffer in log_time order (end of stream).
    fn flush_all<W: Write + Seek>(&mut self, w: &mut Writer<W>) -> Result<()> {
        while let Some((_, m)) = self.buf.pop_first() {
            Self::write(w, &m)?;
        }
        Ok(())
    }
}

pub fn run(args: Args) -> Result<()> {
    let t0 = Instant::now();

    tracing::info!(input = %args.input, output = %args.output, "opening input");

    let in_file = File::open(args.input.as_std_path())
        .with_context(|| format!("open input: {}", args.input))?;
    let mapped = unsafe { memmap2::Mmap::map(&in_file)? };

    let summary = Summary::read(&mapped[..])?
        .ok_or_else(|| anyhow!("input MCAP has no summary; cannot determine channels"))?;

    // Match the input's profile when every schema is ros2msg. This preserves
    // tooling expectations (e.g. Foxglove's ROS 2 panels) without hard-coding.
    let profile = if summary
        .schemas
        .values()
        .all(|s| s.encoding == "ros2msg" || s.encoding.is_empty())
        && !summary.schemas.is_empty()
    {
        "ros2".to_string()
    } else {
        String::new()
    };

    let out_file = File::create(args.output.as_std_path())
        .with_context(|| format!("create output: {}", args.output))?;
    let out_file = BufWriter::with_capacity(4 * 1024 * 1024, out_file);

    let library = format!(
        "slimcap/{}+{}",
        env!("CARGO_PKG_VERSION"),
        args.codec.foxglove_format()
    );
    let mut writer = WriteOptions::new()
        .compression(Some(mcap::Compression::Zstd))
        .profile(profile)
        .library(&library)
        .create(out_file)?;

    // Register the foxglove.CompressedVideo schema once — shared across all
    // color topics. The FDS embeds google.protobuf.Timestamp so downstream
    // tools can fully resolve the type.
    let video_schema_id = writer.add_schema(
        "foxglove.CompressedVideo",
        "protobuf",
        pb::FILE_DESCRIPTOR_SET,
    )?;

    // Remap input schema IDs → output schema IDs for passthrough channels.
    let mut schema_map: HashMap<u16, u16> = HashMap::new();
    for (sid, sch) in &summary.schemas {
        let out_sid = writer.add_schema(&sch.name, &sch.encoding, &sch.data)?;
        schema_map.insert(*sid, out_sid);
    }

    struct TopicState {
        kind: Option<TopicKind>,
        out_channel_id: Option<u16>,
        encoder: Option<TopicEncoder>,
        /// Field-number mapping for protobuf CompressedImage-family inputs;
        /// resolved from the schema's FileDescriptorSet at first-touch.
        image_fields: Option<ImageFields>,

        topic: String,
        in_schema_id: u16,
        in_schema_name: String,
        in_schema_data: Vec<u8>,
        in_message_encoding: String,
        metadata: BTreeMap<String, String>,
    }

    let mut topics: HashMap<u16, TopicState> = HashMap::new();
    for (cid, ch) in &summary.channels {
        let (sid, sname, sdata) = ch
            .schema
            .as_ref()
            .map(|s| (s.id, s.name.clone(), s.data.to_vec()))
            .unwrap_or((0, String::new(), Vec::new()));
        topics.insert(
            *cid,
            TopicState {
                kind: None,
                out_channel_id: None,
                encoder: None,
                image_fields: None,
                topic: ch.topic.clone(),
                in_schema_id: sid,
                in_schema_name: sname,
                in_schema_data: sdata,
                in_message_encoding: ch.message_encoding.clone(),
                metadata: ch.metadata.clone(),
            },
        );
    }

    // Read in log_time order. The raw MessageStream yields file/chunk order,
    // which for overlapping-chunk bags is not even per-topic monotonic; the
    // encoder's VFR pts logic and SLAM consumers both require capture order.
    let stream = OrderedReader::new(&mapped[..], &summary)?;

    let mut total_msgs = 0usize;
    let mut color_msgs = 0usize;
    let mut passthrough_msgs = 0usize;
    let mut color_in_bytes: u64 = 0;
    let mut color_out_bytes: u64 = 0;
    let mut outbuf = OutBuf::new((args.reorder_window_seconds * 1e9) as u64);
    // `--limit-seconds` window. Input is now read in log_time order
    // (OrderedReader), so the window is a contiguous prefix and we can stop as
    // soon as we pass it. Anchor on the bag's true start from summary
    // statistics; saturating_sub guards a degenerate/zero stats value.
    let limit_window_ns: Option<u64> = args.limit_seconds.map(|s| (s * 1e9) as u64);
    let bag_start_ns: Option<u64> = summary
        .stats
        .as_ref()
        .map(|st| st.message_start_time)
        .filter(|&t| t != 0);
    let mut start_log_ns: Option<u64> = None;

    for m in stream {
        let msg = m?;
        let cid = msg.channel_id;

        if let Some(window_ns) = limit_window_ns {
            let base = bag_start_ns
                .unwrap_or_else(|| *start_log_ns.get_or_insert(msg.log_time));
            if msg.log_time.saturating_sub(base) > window_ns {
                break;
            }
        }

        let log_time = msg.log_time;
        let publish_time = msg.publish_time;
        let sequence = msg.sequence;

        let state = topics
            .get_mut(&cid)
            .ok_or_else(|| anyhow!("unexpected channel id {cid}"))?;

        if state.kind.is_none() {
            let resolved = resolve_kind(
                &state.topic,
                &state.in_schema_name,
                &msg.data,
                &args.skip,
                &args.force_color,
            );

            let out_channel_id = match &resolved {
                TopicKind::Color { .. } => {
                    let mut metadata = state.metadata.clone();
                    metadata.insert(
                        "transcoded_from".to_string(),
                        state.in_schema_name.clone(),
                    );
                    writer.add_channel(
                        video_schema_id,
                        &state.topic,
                        "protobuf",
                        &metadata,
                    )?
                }
                TopicKind::Passthrough => {
                    let out_sid = schema_map.get(&state.in_schema_id).copied().unwrap_or(0);
                    writer.add_channel(
                        out_sid,
                        &state.topic,
                        &state.in_message_encoding,
                        &state.metadata,
                    )?
                }
            };
            state.out_channel_id = Some(out_channel_id);

            if let TopicKind::Color { input_schema } = &resolved {
                let enc = TopicEncoder::new(
                    args.codec,
                    args.crf,
                    &args.preset,
                    args.keyframe_interval_seconds,
                    args.bframes,
                )?;
                state.encoder = Some(enc);

                // Resolve protobuf field numbers from the schema data so we
                // tolerate producers with non-canonical numbering.
                state.image_fields = match input_schema {
                    ImageSchema::RosCompressed => None,
                    ImageSchema::FoxgloveCompressed => {
                        match ImageFields::from_fds(
                            &state.in_schema_data,
                            &state.in_schema_name,
                        ) {
                            Ok(f) => {
                                tracing::info!(
                                    topic = %state.topic,
                                    ?f,
                                    "resolved protobuf field numbers from schema"
                                );
                                Some(f)
                            }
                            Err(e) => {
                                tracing::warn!(
                                    topic = %state.topic,
                                    error = %e,
                                    "couldn't read FileDescriptorSet; falling back to canonical numbering"
                                );
                                Some(ImageFields::canonical())
                            }
                        }
                    }
                };

                tracing::info!(
                    topic = %state.topic,
                    schema = %state.in_schema_name,
                    codec = %args.codec,
                    "classified as color → re-encode"
                );
            } else {
                tracing::info!(
                    topic = %state.topic,
                    schema = %state.in_schema_name,
                    "passthrough"
                );
            }
            state.kind = Some(resolved);
        }

        let out_channel_id = state.out_channel_id.unwrap();

        match state.kind.as_ref().unwrap() {
            TopicKind::Passthrough => {
                outbuf.enqueue(QueuedMsg {
                    channel_id: out_channel_id,
                    sequence,
                    log_time,
                    publish_time,
                    data: msg.data,
                });
                passthrough_msgs += 1;
            }
            TopicKind::Color { input_schema } => {
                let (meta, jpeg_bytes): (FrameMeta, Vec<u8>) = match input_schema {
                    ImageSchema::RosCompressed => {
                        let view = cdr::parse_ros_compressed_image(&msg.data)?;
                        let payload = view.payload.to_vec();
                        (
                            FrameMeta {
                                log_time,
                                publish_time,
                                sequence,
                                stamp_sec: view.stamp_sec,
                                stamp_nanos: view.stamp_nanos,
                                frame_id: view.frame_id,
                            },
                            payload,
                        )
                    }
                    ImageSchema::FoxgloveCompressed => {
                        let fields = state
                            .image_fields
                            .expect("image_fields resolved at first touch");
                        let owned = cdr::OwnedFoxgloveImage::decode(&msg.data, fields)?;
                        (
                            FrameMeta {
                                log_time,
                                publish_time,
                                sequence,
                                stamp_sec: owned.stamp_sec,
                                stamp_nanos: owned.stamp_nanos,
                                frame_id: owned.frame_id,
                            },
                            owned.data,
                        )
                    }
                };

                color_in_bytes += jpeg_bytes.len() as u64;

                let encoder = state.encoder.as_mut().expect("encoder present for color");
                let packets = encoder.push_frame(&jpeg_bytes, meta)?;
                for pkt in packets {
                    color_out_bytes += pkt.data.len() as u64;
                    outbuf.enqueue(QueuedMsg {
                        channel_id: out_channel_id,
                        sequence: pkt.meta.sequence,
                        log_time: pkt.meta.log_time,
                        publish_time: pkt.meta.publish_time,
                        data: encode_video_message(&pkt, args.codec)?,
                    });
                    color_msgs += 1;
                }
            }
        }

        total_msgs += 1;
        if total_msgs % args.progress_every == 0 {
            let elapsed = t0.elapsed().as_secs_f64();
            tracing::info!(
                msgs = total_msgs,
                color = color_msgs,
                passthrough = passthrough_msgs,
                color_in_mb = color_in_bytes as f64 / 1e6,
                color_out_mb = color_out_bytes as f64 / 1e6,
                elapsed_s = elapsed,
                "progress"
            );
        }

        // Flush anything now safely behind the encoder's emit latency,
        // keeping the reorder buffer bounded to ~`window` of output.
        outbuf.flush_ready(&mut writer)?;
    }

    let mut tail_color = 0usize;
    for state in topics.values_mut() {
        if let Some(enc) = state.encoder.as_mut() {
            let packets = enc.finish()?;
            let out_channel_id = state.out_channel_id.unwrap();
            for pkt in packets {
                color_out_bytes += pkt.data.len() as u64;
                outbuf.enqueue(QueuedMsg {
                    channel_id: out_channel_id,
                    sequence: pkt.meta.sequence,
                    log_time: pkt.meta.log_time,
                    publish_time: pkt.meta.publish_time,
                    data: encode_video_message(&pkt, args.codec)?,
                });
                tail_color += 1;
            }
        }
    }

    // All frames are now encoded; drain the reorder buffer in log_time order.
    outbuf.flush_all(&mut writer)?;
    writer.finish()?;

    let elapsed = t0.elapsed().as_secs_f64();
    let in_size = std::fs::metadata(args.input.as_std_path())?.len();
    let out_size = std::fs::metadata(args.output.as_std_path())?.len();
    tracing::info!(
        total_msgs,
        color_emitted = color_msgs + tail_color,
        passthrough = passthrough_msgs,
        color_in_mb = color_in_bytes as f64 / 1e6,
        color_out_mb = color_out_bytes as f64 / 1e6,
        in_mib = in_size as f64 / 1024. / 1024.,
        out_mib = out_size as f64 / 1024. / 1024.,
        reduction_pct = (1.0 - out_size as f64 / in_size as f64) * 100.0,
        elapsed_s = elapsed,
        "done"
    );

    Ok(())
}

fn resolve_kind(
    topic: &str,
    schema_name: &str,
    first_payload: &[u8],
    skip: &[String],
    force_color: &[String],
) -> TopicKind {
    if skip.iter().any(|t| t == topic) {
        return TopicKind::Passthrough;
    }
    if force_color.iter().any(|t| t == topic) {
        if let Some(schema) = ImageSchema::from_schema_name(schema_name) {
            return TopicKind::Color { input_schema: schema };
        }
        // Force-color requested on a topic whose schema we don't know how to
        // parse — warn and fall through to normal classification.
        tracing::warn!(
            topic,
            schema_name,
            "force-color requested but schema is not a known image schema; \
             falling back to auto-classification"
        );
    }
    classify_first_message(schema_name, first_payload)
}

/// Serialize one encoded packet as a `foxglove.CompressedVideo` protobuf
/// message body. The MCAP message header (log_time/publish_time/sequence) is
/// applied later by the reorder buffer.
fn encode_video_message(pkt: &EncodedPacket, codec: Codec) -> Result<Vec<u8>> {
    let msg = pb::foxglove::CompressedVideo {
        timestamp: Some(prost_types::Timestamp {
            seconds: pkt.meta.stamp_sec,
            nanos: pkt.meta.stamp_nanos,
        }),
        frame_id: pkt.meta.frame_id.clone(),
        data: pkt.data.clone(),
        format: codec.foxglove_format().to_string(),
    };
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf)?;
    Ok(buf)
}
