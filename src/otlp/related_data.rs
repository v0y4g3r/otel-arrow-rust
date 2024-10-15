// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::decode::record_message::RecordMessage;
use crate::error;
use crate::opentelemetry::arrow::ArrowPayloadType;
use crate::otlp::attribute_store::AttributeStore;
use crate::otlp::data_point_store::{
    EHistogramDataPointsStore, HistogramDataPointsStore, NumberDataPointsStore,
    SummaryDataPointsStore,
};
use crate::otlp::exemplar::ExemplarsStore;

#[derive(Default)]
pub struct RelatedData {
    metric_id: u16,

    pub(crate) res_attr_map_store: AttributeStore<u16>,
    pub(crate) scope_attr_map_store: AttributeStore<u16>,
    number_d_p_attrs_store: AttributeStore<u32>,
    summary_attrs_store: AttributeStore<u32>,
    histogram_attrs_store: AttributeStore<u32>,
    exp_histogram_attrs_store: AttributeStore<u32>,
    number_d_p_exemplar_attrs_store: AttributeStore<u32>,
    histogram_exemplar_attrs_store: AttributeStore<u32>,
    exp_histogram_exemplar_attrs_store: AttributeStore<u32>,

    pub(crate) number_data_points_store: NumberDataPointsStore,
    pub(crate) summary_data_points_store: SummaryDataPointsStore,
    pub(crate) histogram_data_points_store: HistogramDataPointsStore,
    pub(crate) e_histogram_data_points_store: EHistogramDataPointsStore,

    number_data_point_exemplars_store: ExemplarsStore,
    histogram_data_point_exemplars_store: ExemplarsStore,
    e_histogram_data_point_exemplars_store: ExemplarsStore,
}

impl RelatedData {
    pub fn metric_id_from_delta(&mut self, delta: u16) -> u16 {
        self.metric_id += delta;
        self.metric_id
    }
}

pub fn from_record_messages(rbs: &[RecordMessage]) -> error::Result<(RelatedData, Option<usize>)> {
    let mut related_data = RelatedData::default();

    // index for main metrics record.
    let mut metrics_record_idx: Option<usize> = None;

    let mut number_dp_idx: Option<usize> = None;
    let mut summary_dp_idx: Option<usize> = None;
    let mut histogram_dp_idx: Option<usize> = None;
    let mut expHistogram_dp_idx: Option<usize> = None;
    let mut number_dp_ex_idx: Option<usize> = None;
    let mut histogram_dp_ex_idx: Option<usize> = None;
    let mut expHistogram_dp_ex_idx: Option<usize> = None;

    for (idx, rm) in rbs.iter().enumerate() {
        let payload_type = ArrowPayloadType::try_from(rm.payload_type).unwrap();
        match payload_type {
            ArrowPayloadType::Unknown => {
                todo!("error")
            }
            ArrowPayloadType::ResourceAttrs => {
                related_data.res_attr_map_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::ScopeAttrs => {
                related_data.scope_attr_map_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::UnivariateMetrics => {
                // this record is the main metrics record.
                metrics_record_idx = Some(idx);
            }
            ArrowPayloadType::NumberDataPoints => {
                number_dp_idx = Some(idx);
            }
            ArrowPayloadType::SummaryDataPoints => {
                summary_dp_idx = Some(idx);
            }
            ArrowPayloadType::HistogramDataPoints => {
                histogram_dp_idx = Some(idx);
            }
            ArrowPayloadType::ExpHistogramDataPoints => {
                expHistogram_dp_idx = Some(idx);
            }
            ArrowPayloadType::NumberDpAttrs => {
                related_data.number_d_p_attrs_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::SummaryDpAttrs => {
                related_data.summary_attrs_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::HistogramDpAttrs => {
                related_data.histogram_attrs_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::ExpHistogramDpAttrs => {
                related_data.exp_histogram_attrs_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::NumberDpExemplars => {
                number_dp_ex_idx = Some(idx);
            }
            ArrowPayloadType::HistogramDpExemplars => {
                histogram_dp_ex_idx = Some(idx);
            }
            ArrowPayloadType::ExpHistogramDpExemplars => {
                expHistogram_dp_ex_idx = Some(idx);
            }
            ArrowPayloadType::NumberDpExemplarAttrs => {
                related_data.number_d_p_exemplar_attrs_store =
                    AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::HistogramDpExemplarAttrs => {
                related_data.histogram_exemplar_attrs_store = AttributeStore::try_from(&rm.record)?;
            }
            ArrowPayloadType::ExpHistogramDpExemplarAttrs => {
                related_data.exp_histogram_exemplar_attrs_store =
                    AttributeStore::try_from(&rm.record)?;
            }
            _ => unimplemented!(),
        }
    }
    Ok((related_data, metrics_record_idx))
}
