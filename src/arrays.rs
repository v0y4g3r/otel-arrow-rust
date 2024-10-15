use crate::error;
use arrow::array::{
    Array, ArrayAccessor, ArrowPrimitiveType, BinaryArray, BooleanArray, DictionaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray,
    RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::{ArrowDictionaryKeyType, TimeUnit};
use paste::paste;
use snafu::OptionExt;

pub trait NullableArrayAccessor {
    type Native;

    fn value_at(&self, idx: usize) -> Option<Self::Native>;

    fn value_at_or_default(&self, idx: usize) -> Self::Native
    where
        Self::Native: Default,
    {
        self.value_at(idx).unwrap_or_default()
    }
}

impl<T> NullableArrayAccessor for PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    type Native = T::Native;

    fn value_at(&self, idx: usize) -> Option<Self::Native> {
        if self.is_valid(idx) {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl<T> NullableArrayAccessor for Option<&PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
{
    type Native = T::Native;

    fn value_at(&self, idx: usize) -> Option<Self::Native> {
        self.as_ref().and_then(|a| a.value_at(idx))
    }
}

impl NullableArrayAccessor for &BooleanArray {
    type Native = bool;

    fn value_at(&self, idx: usize) -> Option<Self::Native> {
        if self.is_valid(idx) {
            Some(self.value(idx))
        } else {
            None
        }
    }
}

impl NullableArrayAccessor for Option<&BooleanArray> {
    type Native = bool;

    fn value_at(&self, idx: usize) -> Option<Self::Native> {
        self.as_ref().and_then(|a| a.value_at(idx))
    }
}

impl<'a> NullableArrayAccessor for &StringArray {
    type Native = String;

    fn value_at(&self, idx: usize) -> Option<Self::Native> {
        if self.is_valid(idx) {
            Some(self.value(idx).to_string())
        } else {
            None
        }
    }
}

impl<'a> NullableArrayAccessor for &BinaryArray {
    type Native = Vec<u8>;

    fn value_at(&self, idx: usize) -> Option<Self::Native> {
        if self.is_valid(idx) {
            Some(self.value(idx).to_vec())
        } else {
            None
        }
    }
}

macro_rules! impl_downcast {
    ($suffix:ident, $data_type:expr, $array_type:ident) => {
        paste!{
            pub fn [<get_ $suffix _array_opt> ]<'a>(rb: &'a RecordBatch, name: &str) -> error::Result<Option<&'a $array_type>> {
                use arrow::datatypes::DataType::*;
                rb.column_by_name(name)
                    .map(|arr|{
                        arr.as_any()
                            .downcast_ref::<$array_type>()
                            .with_context(|| error::ColumnDataTypeMismatchSnafu {
                                name,
                                expect: $data_type,
                                actual: arr.data_type().clone(),
                            })
                }).transpose()
            }

              pub fn [<get_ $suffix _array> ]<'a>(rb: &'a RecordBatch, name: &str) -> error::Result<&'a $array_type> {
                use arrow::datatypes::DataType::*;
                let arr = rb.column_by_name(name).context(error::ColumnNotFoundSnafu {
            name,
        })?;

                 arr.as_any()
                            .downcast_ref::<$array_type>()
                            .with_context(|| error::ColumnDataTypeMismatchSnafu {
                                name,
                                expect: $data_type,
                                actual: arr.data_type().clone(),
                            })
            }
        }
    };
}

impl_downcast!(u8, UInt8, UInt8Array);
impl_downcast!(u16, UInt16, UInt16Array);
impl_downcast!(u32, UInt32, UInt32Array);
impl_downcast!(u64, UInt64, UInt64Array);
impl_downcast!(i8, Int8, Int8Array);
impl_downcast!(i16, Int16, Int16Array);
impl_downcast!(i32, Int32, Int32Array);
impl_downcast!(i64, Int64, Int64Array);
impl_downcast!(bool, Boolean, BooleanArray);

impl_downcast!(f32, Float32, Float32Array);
impl_downcast!(f64, Float64, Float64Array);

impl_downcast!(string, Utf8, StringArray);
impl_downcast!(binary, Binary, BinaryArray);

impl_downcast!(
    timestamp_nanosecond,
    Timestamp(TimeUnit::Nanosecond, None),
    TimestampNanosecondArray
);

trait NullableInt64ArrayAccessor {
    fn i64_at(&self, idx: usize) -> error::Result<Option<i64>>;
}

impl NullableInt64ArrayAccessor for &Int64Array {
    fn i64_at(&self, idx: usize) -> error::Result<Option<i64>> {
        Ok(self.value_at(idx))
    }
}

impl<T> NullableInt64ArrayAccessor for &DictionaryArray<T>
where
    T: ArrowDictionaryKeyType,
{
    fn i64_at(&self, idx: usize) -> error::Result<Option<i64>> {
        let Some(idx) = self.keys().value_at(idx) else {
            return Ok(None);
        };
        let x = self
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 array");
        let value_idx = idx.to_usize().expect("log");
        Ok(x.value_at(value_idx))
    }
}

trait NullableF64ArrayAccessor {
    fn f64_at(&self, idx: usize) -> error::Result<Option<f64>>;
}

impl NullableF64ArrayAccessor for &Float64Array {
    fn f64_at(&self, idx: usize) -> error::Result<Option<f64>> {
        Ok(self.value_at(idx))
    }
}

impl<T> NullableF64ArrayAccessor for &DictionaryArray<T>
where
    T: ArrowDictionaryKeyType,
{
    fn f64_at(&self, idx: usize) -> error::Result<Option<f64>> {
        let Some(idx) = self.keys().value_at(idx) else {
            return Ok(None);
        };

        let value_idx = idx.to_usize().expect("Invalid value index type");
        let x = self
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 array");
        Ok(x.value_at(value_idx))
    }
}
