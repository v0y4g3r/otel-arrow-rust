use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct DataPointStore<T> {
    next_id: u16,
    data_point_by_id: HashMap<u16, Vec<T>>,
}

impl<T> DataPointStore<T>
where
    T: Default,
{
    pub fn get_or_default(&mut self, key: u16) -> &mut Vec<T> {
        self.data_point_by_id.entry(key).or_default()
    }
}

pub type NumberDataPointsStore = DataPointStore<NumberDataPoint>;
pub type SummaryDataPointsStore = DataPointStore<SummaryDataPoint>;
pub type HistogramDataPointsStore = DataPointStore<HistogramDataPoint>;
pub type EHistogramDataPointsStore = DataPointStore<ExponentialHistogramDataPoint>;
