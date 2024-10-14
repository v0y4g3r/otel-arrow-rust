use arrow::datatypes::DataType;
use num_enum::TryFromPrimitiveError;
use snafu::{Location, Snafu};
use crate::otlp::metric::MetricType;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Cannot find column: {}", name))]
    ColumnNotFound {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display(
        "Column {} data type mismatch, expect: {}, actual: {}",
        name,
        expect,
        actual
    ))]
    ColumnDataTypeMismatch {
        name: String,
        expect: DataType,
        actual: DataType,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot recognize metric type: {}", metric_type))]
    UnrecognizedMetricType {
        metric_type: i32,
        #[snafu(source)]
        error: TryFromPrimitiveError<MetricType>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unable to handle empty metric type"))]
    EmptyMetricType {
        #[snafu(implicit)]
        location: Location,
    }
}
