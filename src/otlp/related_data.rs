use arrow::array::RecordBatch;
use crate::error;
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


pub fn from_record_batches(rbs: &[RecordBatch]) ->error::Result<RelatedData>{
    let related_data = RelatedData::default();


    Ok(related_data)
}