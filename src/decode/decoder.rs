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
use crate::opentelemetry::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};
use crate::otlp::metric::metrics_from;
use crate::otlp::related_data::RelatedData;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use snafu::{ensure, OptionExt, ResultExt};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct Consumer {
    stream_consumers: HashMap<String, StreamConsumer>,
}

struct SharedReader {
    data: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl Read for SharedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut data = self.data.lock().unwrap();
        data.read(buf)
    }
}

pub struct StreamConsumer {
    payload_type: ArrowPayloadType,
    stream_reader: StreamReader<SharedReader>,
    data: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl StreamConsumer {
    fn new(payload: ArrowPayloadType, initial_bytes: Vec<u8>) -> error::Result<Self> {
        let data = Arc::new(Mutex::new(Cursor::new(initial_bytes)));
        let shared_reader = SharedReader { data: data.clone() };
        let stream_reader =
            StreamReader::try_new(shared_reader, None).context(error::BuildStreamReaderSnafu)?;
        Ok(Self {
            payload_type: payload,
            stream_reader,
            data,
        })
    }

    fn replace_bytes(&self, bytes: Vec<u8>) {
        *self.data.lock().unwrap() = Cursor::new(bytes);
    }

    fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        self.stream_reader.next()
    }
}

impl Consumer {
    fn consume_bar(&mut self, bar: &mut BatchArrowRecords) -> error::Result<Vec<RecordMessage>> {
        let mut records = Vec::with_capacity(bar.arrow_payloads.len());

        for payload in std::mem::take(&mut bar.arrow_payloads) {
            let ArrowPayload {
                schema_id,
                r#type,
                record,
            } = payload;
            let payload_type = ArrowPayloadType::try_from(r#type)
                .map_err(|_| error::UnsupportedPayloadTypeSnafu { actual: r#type }.build())?;

            let stream_consumer = match self.stream_consumers.get_mut(&schema_id) {
                None => {
                    // stream consumer does not exist, remove all stream consumer with
                    // the same payload_type since schema already changed for that payload.
                    let new_stream_consumer: HashMap<String, StreamConsumer> =
                        (std::mem::take(&mut self.stream_consumers))
                            .into_iter()
                            .filter(|(_, v)| v.payload_type != payload_type)
                            .collect::<HashMap<_, _>>();
                    self.stream_consumers = new_stream_consumer;
                    self.stream_consumers
                        .entry(schema_id.clone())
                        .or_insert(StreamConsumer::new(payload_type, record)?)
                }
                Some(s) => {
                    // stream consumer exists for given schema id, just reset the bytes.
                    s.replace_bytes(record);
                    s
                }
            };

            if let Some(rs) = stream_consumer.next() {
                // the encoder side ensures there should be only one record here.
                let record = rs.context(error::ReadRecordBatchSnafu)?;
                records.push(RecordMessage {
                    batch_id: bar.batch_id,
                    schema_id,
                    payload_type,
                    record,
                });
            } else {
                //todo: handle stream reader finished
            }
        }
        Ok(records)
    }

    pub fn consume_batches(
        &mut self,
        records: &mut BatchArrowRecords,
    ) -> error::Result<ExportMetricsServiceRequest> {
        ensure!(!records.arrow_payloads.is_empty(), error::EmptyBatchSnafu);

        let main_record_type = records.arrow_payloads[0].r#type;
        let payload_type = ArrowPayloadType::try_from(main_record_type).map_err(|_|
            error::UnsupportedPayloadTypeSnafu {
                actual: main_record_type,
            }.build(),
        )?;
        match payload_type {
            ArrowPayloadType::UnivariateMetrics => {
                let record_message = self.consume_bar(records)?;
                let (mut related_data, metric_record) =
                    RelatedData::from_record_messages(&record_message)?;
                let metric_rec_idx = metric_record.context(error::MetricRecordNotFoundSnafu)?;
                metrics_from(&record_message[metric_rec_idx].record, &mut related_data)
            }

            ArrowPayloadType::Logs => error::UnsupportedPayloadTypeSnafu {
                actual: main_record_type,
            }
            .fail(),
            ArrowPayloadType::Spans => error::UnsupportedPayloadTypeSnafu {
                actual: main_record_type,
            }
            .fail(),
            _ => error::UnsupportedPayloadTypeSnafu {
                actual: main_record_type,
            }
            .fail(),
        }
    }
}
