use crate::arrays::{
    get_f64_array, get_timestamp_nanosecond_array, get_u16_array, get_u32_array, get_u32_array_opt,
    get_u64_array, NullableArrayAccessor,
};
use crate::error;
use crate::otlp::attribute_store::AttributeStore;
use crate::otlp::data_point_store::SummaryDataPointsStore;
use crate::otlp::metric::AppendAndGet;
use crate::schema::consts;
use arrow::array::{Array, ArrayRef, Float64Array, ListArray, RecordBatch, StructArray};
use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
use snafu::OptionExt;

impl SummaryDataPointsStore {
    // see https://github.com/open-telemetry/otel-arrow/blob/985aa1500a012859cec44855e187eacf46eda7c8/pkg/otel/metrics/otlp/summary.go#L117
    pub fn from_record_batch(
        rb: &RecordBatch,
        attr_store: &AttributeStore<u32>,
    ) -> error::Result<SummaryDataPointsStore> {
        let mut store = SummaryDataPointsStore::default();
        let mut prev_parent_id = 0;

        let id_arr_opt = get_u32_array_opt(rb, consts::ID)?;
        let delta_id_arr = get_u16_array(rb, consts::ParentID)?;
        let start_time_unix_nano_arr =
            get_timestamp_nanosecond_array(rb, consts::StartTimeUnixNano)?;
        let time_unix_nano_arr = get_timestamp_nanosecond_array(rb, consts::TimeUnixNano)?;
        let summary_count_arr = get_u64_array(rb, consts::SummaryCount)?;
        let sum_arr = get_f64_array(rb, consts::SummarySum)?;
        let quantile_arr =
            QuantileArrays::try_new(rb.column_by_name(consts::SummaryQuantileValues).context(
                error::ColumnNotFoundSnafu {
                    name: consts::SummaryQuantileValues,
                },
            )?)?;
        let flag_arr = get_u32_array(rb, consts::Flags)?;

        for idx in 0..rb.num_rows() {
            let delta = delta_id_arr.value_at_or_default(idx);
            let parent_id = prev_parent_id + delta;
            prev_parent_id = parent_id;
            let nbdps = store.get_or_default(parent_id);

            let mut sdp = nbdps.append_and_get();
            sdp.start_time_unix_nano = start_time_unix_nano_arr.value_at_or_default(idx) as u64;
            sdp.time_unix_nano = time_unix_nano_arr.value_at_or_default(idx) as u64;
            sdp.count = summary_count_arr.value_at_or_default(idx);
            sdp.sum = sum_arr.value_at_or_default(idx);
            if let Some(quantile) = quantile_arr.value_at(idx) {
                sdp.quantile_values = quantile;
            }
            sdp.flags = flag_arr.value_at_or_default(idx);
            if let Some(id) = id_arr_opt.value_at(idx)
                && let Some(attr) = attr_store.attribute_by_id(id)
            {
                sdp.attributes = attr.to_vec();
            }
        }

        Ok(store)
    }
}

struct QuantileArrays<'a> {
    list_array: &'a ListArray,
    quantile_array: &'a Float64Array,
    value_array: &'a Float64Array,
}

impl<'a> QuantileArrays<'a> {
    fn try_new(array: &'a ArrayRef) -> error::Result<Self> {
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .with_context(|| error::InvalidQuantileTypeSnafu {
                message: array.data_type().to_string(),
            })?;

        let struct_array = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .with_context(|| error::InvalidQuantileTypeSnafu {
                message: array.data_type().to_string(),
            })?;
        let downcast_f64 =
            |struct_array: &'a StructArray, name: &str| -> error::Result<&'a Float64Array> {
                let field_column = struct_array
                    .column_by_name(name)
                    .context(error::ColumnNotFoundSnafu { name })?;

                field_column
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .with_context(|| error::InvalidQuantileTypeSnafu {
                        message: field_column.data_type().to_string(),
                    })
            };

        let quantile = downcast_f64(struct_array, consts::SummaryQuantile)?;
        let value = downcast_f64(struct_array, consts::SummaryValue)?;
        assert_eq!(value.len(), quantile.len());
        Ok(Self {
            list_array: list,
            quantile_array: quantile,
            value_array: value,
        })
    }
}

impl<'a> QuantileArrays<'a> {
    fn value_at(&self, idx: usize) -> Option<Vec<ValueAtQuantile>> {
        if !self.list_array.is_valid(idx) {
            return None;
        }
        let start = self.list_array.value_offsets()[idx];
        let end = self.list_array.value_offsets()[idx + 1];

        let quantiles = (start..end)
            .map(|idx| {
                let idx = idx as usize;
                ValueAtQuantile {
                    quantile: self.quantile_array.value_at_or_default(idx),
                    value: self.value_array.value_at_or_default(idx),
                }
            })
            .collect::<Vec<_>>();
        Some(quantiles)
    }
}
