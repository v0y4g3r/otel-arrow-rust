use opentelemetry_proto::tonic::common::v1::KeyValue;
use std::collections::HashMap;
use std::hash::Hash;

pub struct AttributeStore<T> {
    last_id: T,
    attribute_by_ids: HashMap<T, Vec<KeyValue>>,
}

impl<T> AttributeStore<T>
where
    T: Hash + Eq,
{
    pub fn attribute_by_id(&self, id: T) -> Option<&[KeyValue]> {
        self.attribute_by_ids.get(&id).map(|r| r.as_slice())
    }
}
