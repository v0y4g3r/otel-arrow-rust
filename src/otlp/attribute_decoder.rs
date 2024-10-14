// https://github.com/open-telemetry/otel-arrow/blob/985aa1500a012859cec44855e187eacf46eda7c8/pkg/otel/common/arrow/attributes.go#L40

use crate::arrays::NullableArrayAccessor;
use arrow::array::{Array, UInt16Array, UInt32Array};
use arrow::datatypes::DataType;
use num_enum::TryFromPrimitive;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue};
use std::hash::Hash;

pub trait ParentId: Copy + Hash + Eq + Default {
    type Array: NullableArrayAccessor<Native = Self>;
    fn add(self, other: Self) -> Self;

    fn arrow_data_type() -> DataType;

    fn new_decoder() -> AttrsParentIdDecoder<Self>;
}


impl ParentId for u16 {
    type Array = UInt16Array;

    fn add(self, other: Self) -> Self {
        self + other
    }

    fn arrow_data_type() -> DataType {
        DataType::UInt16
    }

    fn new_decoder() -> AttrsParentIdDecoder<Self> {
        Attrs16ParentIdDecoder::default()
    }
}

impl ParentId for u32 {
    type Array = UInt32Array;

    fn add(self, other: Self) -> Self {
        self + other
    }

    fn arrow_data_type() -> DataType {
        DataType::UInt32
    }

    fn new_decoder() -> AttrsParentIdDecoder<Self> {
        Attrs32ParentIdDecoder::default()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, TryFromPrimitive)]
#[repr(u8)]
pub enum ParentIdEncoding {
    /// ParentIdNoEncoding stores the parent ID as is.
    ParentIdNoEncoding = 0,
    /// ParentIdDeltaEncoding stores the parent ID as a delta from the previous
    /// parent ID.
    ParentIdDeltaEncoding = 1,
    /// ParentIdDeltaGroupEncoding stores the parent ID as a delta from the
    /// previous parent ID in the same group. A group is defined by the
    /// combination Key and Value.
    ParentIdDeltaGroupEncoding = 2,
}

pub type Attrs16ParentIdDecoder = AttrsParentIdDecoder<u16>;
pub type Attrs32ParentIdDecoder = AttrsParentIdDecoder<u32>;

pub struct AttrsParentIdDecoder<T> {
    encoding_type: ParentIdEncoding,
    prev_parent_id: T,
    prev_key: Option<String>,
    prev_value: Option<any_value::Value>,
}

impl<T> Default for AttrsParentIdDecoder<T>
where
    T: ParentId,
{
    fn default() -> Self {
        Self {
            encoding_type: ParentIdEncoding::ParentIdDeltaGroupEncoding,
            prev_parent_id: T::default(),
            prev_key: None,
            prev_value: None,
        }
    }
}

impl<T> AttrsParentIdDecoder<T>
where
    T: ParentId,
{
    pub fn decode(&mut self, deltaOrParentID: T, key: &str, value: &any_value::Value) -> T {
        match self.encoding_type {
            // Plain encoding
            ParentIdEncoding::ParentIdNoEncoding => deltaOrParentID,
            /// Simply delta
            ParentIdEncoding::ParentIdDeltaEncoding => {
                let decode_parent_id = self.prev_parent_id.add(deltaOrParentID);
                self.prev_parent_id = decode_parent_id;
                decode_parent_id
            }
            /// Key-value scoped delta.
            ParentIdEncoding::ParentIdDeltaGroupEncoding => {
                if self.prev_key.as_deref() == Some(key) && self.prev_value.as_ref() == Some(&value)
                {
                    let parent_id = self.prev_parent_id.add(deltaOrParentID);
                    self.prev_parent_id = parent_id;
                    parent_id
                } else {
                    self.prev_key = Some(key.to_string());
                    self.prev_value = Some(value.clone());
                    self.prev_parent_id = deltaOrParentID;
                    deltaOrParentID
                }
            }
        }
    }
}
