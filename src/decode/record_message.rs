use crate::opentelemetry::arrow::ArrowPayloadType;
use arrow::array::RecordBatch;

/// Wrapper for [RecordBatch].
pub struct RecordMessage {
    pub(crate) batch_id: i64,
    pub(crate) schema_id: String,
    pub(crate) payload_type: ArrowPayloadType,
    pub(crate) record: RecordBatch,
}
