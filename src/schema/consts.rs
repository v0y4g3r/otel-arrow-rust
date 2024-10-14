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

pub const ID: &str = "id";
pub const ParentID: &str = "parent_id";

pub const MetricType: &str = "metric_type";

pub const ResourceMetrics: &str = "resource_metrics";
pub const TimeUnixNano: &str = "time_unix_nano";
pub const StartTimeUnixNano: &str = "start_time_unix_nano";
pub const DurationTimeUnixNano: &str = "duration_time_unix_nano";
pub const ObservedTimeUnixNano: &str = "observed_time_unix_nano";
pub const SeverityNumber: &str = "severity_number";
pub const SeverityText: &str = "severity_text";
pub const DroppedAttributesCount: &str = "dropped_attributes_count";
pub const DroppedEventsCount: &str = "dropped_events_count";
pub const DroppedLinksCount: &str = "dropped_links_count";
pub const Flags: &str = "flags";
pub const TraceId: &str = "trace_id";
pub const TraceState: &str = "trace_state";
pub const SpanId: &str = "span_id";
pub const ParentSpanId: &str = "parent_span_id";
pub const Attributes: &str = "attributes";
pub const Resource: &str = "resource";
pub const ScopeMetrics: &str = "scope_metrics";
pub const Scope: &str = "scope";
pub const Name: &str = "name";
pub const KIND: &str = "kind";
pub const Version: &str = "version";
pub const Body: &str = "body";
pub const Status: &str = "status";
pub const Description: &str = "description";
pub const Unit: &str = "unit";
pub const Data: &str = "data";
pub const StatusMessage: &str = "status_message";
pub const StatusCode: &str = "code";
pub const SummaryCount: &str = "count";
pub const SummarySum: &str = "sum";
pub const SummaryQuantileValues: &str = "quantile";
pub const SummaryQuantile: &str = "quantile";
pub const SummaryValue: &str = "value";
pub const MetricValue: &str = "value";
pub const IntValue: &str = "int_value";
pub const DoubleValue: &str = "double_value";
pub const HistogramCount: &str = "count";
pub const HistogramSum: &str = "sum";
pub const HistogramMin: &str = "min";
pub const HistogramMax: &str = "max";
pub const HistogramBucketCounts: &str = "bucket_counts";
pub const HistogramExplicitBounds: &str = "explicit_bounds";
pub const ExpHistogramScale: &str = "scale";
pub const ExpHistogramZeroCount: &str = "zero_count";
pub const ExpHistogramPositive: &str = "positive";
pub const ExpHistogramNegative: &str = "negative";
pub const ExpHistogramOffset: &str = "offset";
pub const ExpHistogramBucketCounts: &str = "bucket_counts";
pub const SchemaUrl: &str = "schema_url";
pub const I64MetricValue: &str = "i64";
pub const F64MetricValue: &str = "f64";
pub const Exemplars: &str = "exemplars";
pub const IsMonotonic: &str = "is_monotonic";
pub const AggregationTemporality: &str = "aggregation_temporality";

pub const AttributeKey: &str = "key";
pub const AttributeType: &str = "type";
pub const AttributeStr: &str = "str";
pub const AttributeInt: &str = "int";
pub const AttributeDouble: &str = "double";
pub const AttributeBool: &str = "bool";
pub const AttributeBytes: &str = "bytes";
pub const AttributeSer: &str = "ser";
