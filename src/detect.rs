//! Auto-classify topics into Color (→ H.264) / Passthrough. The rule is
//! deliberately conservative: a topic must be an image schema AND the first
//! sampled payload must be JPEG. Anything else (PNG depth, PNG color, empty,
//! unknown codec) falls through to passthrough so we never silently corrupt
//! data.

use crate::cdr;

/// Kind of image schema carried on a channel. Anything not in this set is
/// classified as [`TopicKind::Passthrough`] without further inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageSchema {
    /// `sensor_msgs/msg/CompressedImage` (ros2msg / CDR).
    RosCompressed,
    /// `foxglove.CompressedImage` (protobuf).
    FoxgloveCompressed,
}

impl ImageSchema {
    pub fn from_schema_name(name: &str) -> Option<Self> {
        match name {
            "sensor_msgs/msg/CompressedImage" => Some(ImageSchema::RosCompressed),
            "sensor_msgs/CompressedImage" => Some(ImageSchema::RosCompressed),
            "foxglove.CompressedImage" => Some(ImageSchema::FoxgloveCompressed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum TopicKind {
    /// Re-encode this topic to H.264 `foxglove.CompressedVideo`.
    Color { input_schema: ImageSchema },
    /// Copy the message bytes through unchanged.
    Passthrough,
}

/// Peek at a message payload to determine whether this topic should be
/// transcoded. Must be given the *first* message seen on the topic; caller
/// caches the result for the rest of the stream.
///
/// We do NOT rely on compiled-in protobuf field numbers here — the microagi
/// bag, for instance, numbers `data` as field 2 rather than the canonical 3.
/// Instead we scan for any length-delimited field whose payload starts with
/// JPEG magic.
pub fn classify_first_message(schema_name: &str, raw_msg_data: &[u8]) -> TopicKind {
    let Some(schema) = ImageSchema::from_schema_name(schema_name) else {
        return TopicKind::Passthrough;
    };

    let is_jpeg_input = match schema {
        ImageSchema::RosCompressed => cdr::parse_ros_compressed_image(raw_msg_data)
            .map(|v| starts_with_jpeg(v.payload))
            .unwrap_or(false),
        ImageSchema::FoxgloveCompressed => cdr::protobuf_has_jpeg_payload(raw_msg_data),
    };

    if is_jpeg_input {
        TopicKind::Color { input_schema: schema }
    } else {
        TopicKind::Passthrough
    }
}

fn starts_with_jpeg(data: &[u8]) -> bool {
    data.len() >= 3 && data[0] == 0xff && data[1] == 0xd8 && data[2] == 0xff
}
