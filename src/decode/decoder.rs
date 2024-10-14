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
use crate::error::Error;
use crate::opentelemetry::arrow::{ArrowPayloadType,};

pub struct RelatedData {

}

impl TryFrom<&[RecordMessage]> for RelatedData {
    type Error = Error;

    fn try_from(records: &[RecordMessage]) -> Result<Self, Self::Error> {
        for record in records {
            match record.payload_type {
                ArrowPayloadType::Unknown => {
                    unimplemented!()
                }
                ArrowPayloadType::ResourceAttrs => {
                    unimplemented!()
                }
                ArrowPayloadType::ScopeAttrs => {
                    unimplemented!()
                }
                ArrowPayloadType::UnivariateMetrics => {}
                ArrowPayloadType::NumberDataPoints => {}
                ArrowPayloadType::SummaryDataPoints => {}
                ArrowPayloadType::HistogramDataPoints => {}
                ArrowPayloadType::ExpHistogramDataPoints => {}
                ArrowPayloadType::NumberDpAttrs => {}
                ArrowPayloadType::SummaryDpAttrs => {}
                ArrowPayloadType::HistogramDpAttrs => {}
                ArrowPayloadType::ExpHistogramDpAttrs => {}
                ArrowPayloadType::NumberDpExemplars => {}
                ArrowPayloadType::HistogramDpExemplars => {}
                ArrowPayloadType::ExpHistogramDpExemplars => {}
                ArrowPayloadType::NumberDpExemplarAttrs => {}
                ArrowPayloadType::HistogramDpExemplarAttrs => {}
                ArrowPayloadType::ExpHistogramDpExemplarAttrs => {}
                ArrowPayloadType::MultivariateMetrics => {}
                ArrowPayloadType::Logs => {}
                ArrowPayloadType::LogAttrs => {}
                ArrowPayloadType::Spans => {}
                ArrowPayloadType::SpanAttrs => {}
                ArrowPayloadType::SpanEvents => {}
                ArrowPayloadType::SpanLinks => {}
                ArrowPayloadType::SpanEventAttrs => {}
                ArrowPayloadType::SpanLinkAttrs => {}
            }
        }
        Ok(Self{})
    }

}

