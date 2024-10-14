use std::collections::HashMap;
use opentelemetry_proto::tonic::metrics::v1::Exemplar;

pub struct ExemplarsStore {
    next_id: u32,
    exemplars_by_ids:  HashMap<u32, Vec<Exemplar>>
}

impl ExemplarsStore {
    /// Gets or creates the exemplar of given id and creates a new one if not yet created.
    pub fn get_or_create_exemplar_by_id(&mut self, id: u32) -> &mut Vec<Exemplar> {
        self.exemplars_by_ids.entry(id).or_default()
    }
}