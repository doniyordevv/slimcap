//! Minimal parsers for `sensor_msgs/CompressedImage` (ROS 2 CDR little-endian)
//! and `foxglove.CompressedImage`-family protobuf messages.
//!
//! The protobuf path is schema-aware: some producers (e.g. microagi) use
//! different field numbers than Foxglove's canonical `.proto`, so we look up
//! field numbers dynamically from the `FileDescriptorSet` embedded in the
//! MCAP schema record and parse the wire format by hand.

use anyhow::{anyhow, bail, Result};
use prost::Message as _;
use prost_types::{FileDescriptorSet, field_descriptor_proto::Type as PbType};

/// Borrowed view of a compressed-image message — no allocations beyond what
/// the parser already has (protobuf decode still decodes the varint-tagged
/// frame).
pub struct CompressedImageView<'a> {
    pub stamp_sec: i64,
    pub stamp_nanos: i32,
    pub frame_id: String,
    pub payload: &'a [u8],
}

/// Parse ROS 2 CDR-encapsulated `sensor_msgs/msg/CompressedImage`.
///
/// Layout (CDR-LE after the 4-byte encapsulation header):
/// ```text
///   int32  stamp.sec
///   uint32 stamp.nanosec
///   uint32 frame_id length  (includes trailing NUL)
///   bytes  frame_id + NUL
///   <align to 4>
///   uint32 format length
///   bytes  format + NUL
///   <align to 4>
///   uint32 data length
///   bytes  data
/// ```
pub fn parse_ros_compressed_image(buf: &[u8]) -> Result<CompressedImageView<'_>> {
    if buf.len() < 4 {
        bail!("CDR buffer too short for encap header");
    }
    // Skip 4-byte encapsulation header (0x00 0x01 0x00 0x00 = CDR_LE, options 0).
    let mut off = 4usize;

    let sec = read_i32_le(buf, &mut off)?;
    let nsec = read_u32_le(buf, &mut off)?;

    let fid_len = read_u32_le(buf, &mut off)? as usize;
    let frame_id = read_cdr_string(buf, &mut off, fid_len)?;
    align_to(&mut off, 4);

    let fmt_len = read_u32_le(buf, &mut off)? as usize;
    // The `format` string is read for offset advancement only; slimcap
    // classifies on JPEG magic in the payload, not on the declared format.
    let _format = read_cdr_string(buf, &mut off, fmt_len)?;
    align_to(&mut off, 4);

    let data_len = read_u32_le(buf, &mut off)? as usize;
    if off.saturating_add(data_len) > buf.len() {
        bail!("CDR data length {} overruns buffer", data_len);
    }
    let payload = &buf[off..off + data_len];

    Ok(CompressedImageView {
        stamp_sec: sec as i64,
        stamp_nanos: nsec as i32,
        frame_id,
        payload,
    })
}

/// Field numbers for a CompressedImage-style protobuf message, resolved from
/// the schema's FileDescriptorSet.
///
/// Foxglove's canonical numbering:   timestamp=1, frame_id=2, data=3, format=4
/// Some producers (microagi) use:    timestamp=1, data=2, format=3, frame_id=4
#[derive(Debug, Clone, Copy)]
pub struct ImageFields {
    pub timestamp: Option<u32>,
    pub frame_id: Option<u32>,
    pub data: u32,
    pub format: Option<u32>,
}

impl ImageFields {
    /// Default to Foxglove's canonical numbering — used when the schema data
    /// is missing or we can't find the message in it.
    pub fn canonical() -> Self {
        Self {
            timestamp: Some(1),
            frame_id: Some(2),
            data: 3,
            format: Some(4),
        }
    }

    /// Look up field numbers for a message type inside a FileDescriptorSet.
    /// `message_name` is the fully-qualified proto name, e.g.
    /// `foxglove.CompressedImage`.
    pub fn from_fds(fds_bytes: &[u8], message_name: &str) -> Result<Self> {
        let fds = FileDescriptorSet::decode(fds_bytes)
            .map_err(|e| anyhow!("decode FileDescriptorSet: {e}"))?;
        let (pkg_wanted, name_wanted) = match message_name.rsplit_once('.') {
            Some((p, n)) => (p.to_string(), n.to_string()),
            None => (String::new(), message_name.to_string()),
        };
        for file in &fds.file {
            if file.package() != pkg_wanted {
                continue;
            }
            for mt in &file.message_type {
                if mt.name() != name_wanted {
                    continue;
                }
                let mut fields = ImageFields {
                    timestamp: None,
                    frame_id: None,
                    data: 0,
                    format: None,
                };
                let mut found_data = false;
                for f in &mt.field {
                    let number = f.number() as u32;
                    match f.name() {
                        "timestamp" if f.r#type() == PbType::Message => {
                            fields.timestamp = Some(number);
                        }
                        "frame_id" if f.r#type() == PbType::String => {
                            fields.frame_id = Some(number);
                        }
                        "data" if f.r#type() == PbType::Bytes => {
                            fields.data = number;
                            found_data = true;
                        }
                        "format" if f.r#type() == PbType::String => {
                            fields.format = Some(number);
                        }
                        _ => {}
                    }
                }
                if !found_data {
                    bail!(
                        "message {} has no `bytes data` field (cannot locate image payload)",
                        message_name
                    );
                }
                return Ok(fields);
            }
        }
        bail!("message {} not found in FileDescriptorSet", message_name)
    }
}

/// Decode a `foxglove.CompressedImage`-style protobuf message into an owned
/// view using the given field-number mapping.
pub struct OwnedFoxgloveImage {
    pub stamp_sec: i64,
    pub stamp_nanos: i32,
    pub frame_id: String,
    pub format: String,
    pub data: Vec<u8>,
}

impl OwnedFoxgloveImage {
    /// Decode with explicit field numbers (schema-aware).
    pub fn decode(buf: &[u8], fields: ImageFields) -> Result<Self> {
        let mut out = OwnedFoxgloveImage {
            stamp_sec: 0,
            stamp_nanos: 0,
            frame_id: String::new(),
            format: String::new(),
            data: Vec::new(),
        };

        for item in WireIter::new(buf) {
            let (field_no, payload) = item?;
            if Some(field_no) == fields.timestamp {
                if let WirePayload::Length(bytes) = payload {
                    // Timestamp is `int64 seconds = 1; int32 nanos = 2;`
                    for ts_item in WireIter::new(bytes) {
                        let (ts_field, ts_payload) = ts_item?;
                        if let WirePayload::Varint(v) = ts_payload {
                            match ts_field {
                                1 => out.stamp_sec = v as i64,
                                2 => out.stamp_nanos = v as i32,
                                _ => {}
                            }
                        }
                    }
                }
            } else if Some(field_no) == fields.frame_id {
                if let WirePayload::Length(bytes) = payload {
                    out.frame_id = String::from_utf8_lossy(bytes).into_owned();
                }
            } else if field_no == fields.data {
                if let WirePayload::Length(bytes) = payload {
                    out.data = bytes.to_vec();
                }
            } else if Some(field_no) == fields.format {
                if let WirePayload::Length(bytes) = payload {
                    out.format = String::from_utf8_lossy(bytes).into_owned();
                }
            }
        }
        Ok(out)
    }
}

/// Lightweight protobuf wire-format scanner. Yields `(field_number, payload)`
/// for each field. No UTF-8 validation or field-type checking — used both for
/// schema-aware extraction (where the caller knows what to do with each field)
/// and for generic scanning (e.g. "find any bytes field starting with JPEG
/// magic" during classification).
///
/// `Fixed64` / `Fixed32` carry the raw bytes; we never inspect them but
/// retaining the payload keeps the variant useful for future callers without
/// changing the API later.
#[allow(dead_code)]
pub enum WirePayload<'a> {
    Varint(u64),
    Fixed64([u8; 8]),
    Length(&'a [u8]),
    Fixed32([u8; 4]),
}

pub struct WireIter<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireIter<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
}

impl<'a> Iterator for WireIter<'a> {
    type Item = Result<(u32, WirePayload<'a>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let tag = match read_varint(self.buf, &mut self.pos) {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let field_no = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        let payload = match wire {
            0 => match read_varint(self.buf, &mut self.pos) {
                Ok(v) => WirePayload::Varint(v),
                Err(e) => return Some(Err(e)),
            },
            1 => {
                if self.pos + 8 > self.buf.len() {
                    return Some(Err(anyhow!("short read for fixed64")));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
                self.pos += 8;
                WirePayload::Fixed64(arr)
            }
            2 => {
                let len = match read_varint(self.buf, &mut self.pos) {
                    Ok(v) => v as usize,
                    Err(e) => return Some(Err(e)),
                };
                if self.pos + len > self.buf.len() {
                    return Some(Err(anyhow!("length-delim field overruns buffer")));
                }
                let slice = &self.buf[self.pos..self.pos + len];
                self.pos += len;
                WirePayload::Length(slice)
            }
            5 => {
                if self.pos + 4 > self.buf.len() {
                    return Some(Err(anyhow!("short read for fixed32")));
                }
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&self.buf[self.pos..self.pos + 4]);
                self.pos += 4;
                WirePayload::Fixed32(arr)
            }
            w => return Some(Err(anyhow!("unsupported wire type {w}"))),
        };
        Some(Ok((field_no, payload)))
    }
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut v: u64 = 0;
    let mut shift = 0;
    loop {
        let b = *buf
            .get(*pos)
            .ok_or_else(|| anyhow!("short varint at offset {}", *pos))?;
        *pos += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint too long");
        }
    }
}

/// Scan any protobuf message for a length-delimited field that starts with
/// JPEG magic. Used by classification when we don't have (or don't want to
/// trust) the schema field numbers.
pub fn protobuf_has_jpeg_payload(buf: &[u8]) -> bool {
    for item in WireIter::new(buf) {
        let Ok((_, payload)) = item else { return false };
        if let WirePayload::Length(bytes) = payload {
            if bytes.len() >= 3 && bytes[0] == 0xff && bytes[1] == 0xd8 && bytes[2] == 0xff {
                return true;
            }
        }
    }
    false
}

fn read_i32_le(buf: &[u8], off: &mut usize) -> Result<i32> {
    let slice = buf
        .get(*off..*off + 4)
        .ok_or_else(|| anyhow!("short read at offset {}", *off))?;
    *off += 4;
    Ok(i32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u32_le(buf: &[u8], off: &mut usize) -> Result<u32> {
    let slice = buf
        .get(*off..*off + 4)
        .ok_or_else(|| anyhow!("short read at offset {}", *off))?;
    *off += 4;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_cdr_string(buf: &[u8], off: &mut usize, len: usize) -> Result<String> {
    if len == 0 {
        return Ok(String::new());
    }
    let slice = buf
        .get(*off..*off + len)
        .ok_or_else(|| anyhow!("short string read (len={}) at offset {}", len, *off))?;
    *off += len;
    // CDR strings are NUL-terminated; trim it.
    let end = if slice.last() == Some(&0) { len - 1 } else { len };
    Ok(String::from_utf8_lossy(&slice[..end]).into_owned())
}

fn align_to(off: &mut usize, n: usize) {
    *off = (*off + n - 1) & !(n - 1);
}
