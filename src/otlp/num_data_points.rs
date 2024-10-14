use crate::arrays::{
    get_f64_array, get_i64_array, get_timestamp_nanosecond_array, get_u16_array, get_u32_array,
    NullableArrayAccessor,
};
use crate::error::Result;
use crate::otlp::attribute_store::AttributeStore;
use crate::otlp::data_point_store::NumberDataPointsStore;
use crate::otlp::exemplar::ExemplarsStore;
use crate::schema::consts;
use arrow::array::RecordBatch;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use opentelemetry_proto::tonic::metrics::v1::NumberDataPoint;

/// Ref: https://github.com/open-telemetry/otel-arrow/blob/985aa1500a012859cec44855e187eacf46eda7c8/pkg/otel/metrics/otlp/number_data_point.go#L110
pub fn from_record_batch(
    rb: &RecordBatch,
    exemplar_store: &mut ExemplarsStore,
    attribute_store: &mut AttributeStore<u32>,
) -> Result<NumberDataPointsStore> {
    let mut store = NumberDataPointsStore::default();

    let id_array = get_u32_array(rb, consts::ID)?;
    let parent_id_array = get_u16_array(rb, consts::ParentID)?;
    let start_time_unix_nano_array = get_timestamp_nanosecond_array(rb, consts::StartTimeUnixNano)?;
    let time_unix_nano_array = get_timestamp_nanosecond_array(rb, consts::TimeUnixNano)?;

    // todo(hl): The receiver code of otelarrow also handles dictionary arrays for int_value field
    // but the exporter side seems only encode to Int64Array: https://github.com/open-telemetry/otel-arrow/blob/79b50d99dde17c5bb085a0204db406d8f6ad880b/pkg/otel/metrics/arrow/number_data_point.go#L138
    let int_value = get_i64_array(rb, consts::IntValue)?;
    let double_value = get_f64_array(rb, consts::DoubleValue)?;
    let flags = get_u32_array(rb, consts::Flags)?;

    let mut last_id = 0;
    let mut prev_parent_id = 0;

    for idx in 0..rb.num_rows() {
        let id = id_array.value_at(idx);
        let delta = parent_id_array.value_at(idx).unwrap_or_default();
        let parent_id = prev_parent_id + delta;
        prev_parent_id = parent_id;

        let nbdps = store.get_or_default(parent_id);
        let mut nbdp = NumberDataPoint::default();
        nbdp.start_time_unix_nano =
            start_time_unix_nano_array.value_at(idx).unwrap_or_default() as u64;
        nbdp.time_unix_nano = time_unix_nano_array.value_at(idx).unwrap_or_default() as u64;

        match (int_value.value_at(idx), double_value.value_at(idx)) {
            (Some(int), None) => {
                nbdp.value = Some(Value::AsInt(int));
            }
            (None, Some(double)) => {
                nbdp.value = Some(Value::AsDouble(double));
            }
            (Some(_), Some(_)) => {
                panic!("unexpected")
            }
            (None, None) => {
                nbdp.value = None;
            }
        }

        nbdp.flags = flags.value_at(idx).unwrap_or_default();
        if let Some(id) = id {
            last_id += id;
            let exemplars = exemplar_store.get_or_create_exemplar_by_id(last_id);
            nbdp.exemplars.extend(std::mem::take(exemplars));

            if let Some(attr) = attribute_store.attribute_by_id(last_id) {
                nbdp.attributes = attr.to_vec();
            }
        }
        nbdps.push(nbdp);
    }

    Ok(store)
}
