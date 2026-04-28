//! End-to-end streaming pipeline: read input MCAP, classify each channel on
//! first sight, re-encode color topics, passthrough the rest, write a new
//! MCAP with zstd-chunked output.

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::time::Instant;

use anyhow::{anyhow, Context as _, Result};
use mcap::records::MessageHeader;
use mcap::{MessageStream, Summary, WriteOptions, Writer};
use prost::Message as _;

use crate::cdr::{self, ImageFields};
use crate::cli::Args;
use crate::detect::{classify_first_message, ImageSchema, TopicKind};
use crate::encoder::{EncodedPacket, FrameMeta, TopicEncoder};
use crate::pb;

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

    let mut writer = WriteOptions::new()
        .compression(Some(mcap::Compression::Zstd))
        .profile(profile)
        .library("slimcap/0.1.0+h264")
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

    let stream = MessageStream::new(&mapped[..])?;

    let mut total_msgs = 0usize;
    let mut color_msgs = 0usize;
    let mut passthrough_msgs = 0usize;
    let mut color_in_bytes: u64 = 0;
    let mut color_out_bytes: u64 = 0;
    let mut start_log_ns: Option<u64> = None;

    for m in stream {
        let msg = m?;
        let cid = msg.channel.id;

        if start_log_ns.is_none() {
            start_log_ns = Some(msg.log_time);
        }
        if let Some(limit) = args.limit_seconds {
            let elapsed_s = (msg.log_time - start_log_ns.unwrap()) as f64 / 1e9;
            if elapsed_s > limit {
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
                    args.crf,
                    &args.preset,
                    args.keyframe_interval_seconds,
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
                    "classified as color → H.264"
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
                writer.write_to_known_channel(
                    &MessageHeader {
                        channel_id: out_channel_id,
                        sequence,
                        log_time,
                        publish_time,
                    },
                    &msg.data,
                )?;
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
                    emit_video_packet(&mut writer, out_channel_id, &pkt)?;
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
    }

    let mut tail_color = 0usize;
    for state in topics.values_mut() {
        if let Some(enc) = state.encoder.as_mut() {
            let packets = enc.finish()?;
            let out_channel_id = state.out_channel_id.unwrap();
            for pkt in packets {
                color_out_bytes += pkt.data.len() as u64;
                emit_video_packet(&mut writer, out_channel_id, &pkt)?;
                tail_color += 1;
            }
        }
    }

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

fn emit_video_packet<W: Write + Seek>(
    writer: &mut Writer<W>,
    channel_id: u16,
    pkt: &EncodedPacket,
) -> Result<()> {
    let msg = pb::foxglove::CompressedVideo {
        timestamp: Some(prost_types::Timestamp {
            seconds: pkt.meta.stamp_sec,
            nanos: pkt.meta.stamp_nanos,
        }),
        frame_id: pkt.meta.frame_id.clone(),
        data: pkt.data.clone(),
        format: "h264".to_string(),
    };
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf)?;
    writer.write_to_known_channel(
        &MessageHeader {
            channel_id,
            sequence: pkt.meta.sequence,
            log_time: pkt.meta.log_time,
            publish_time: pkt.meta.publish_time,
        },
        &buf,
    )?;
    Ok(())
}
