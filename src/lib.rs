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

#![feature(option_get_or_insert_default)]
#![feature(let_chains)]

#[allow(dead_code)]
pub(crate) mod arrays;
mod decode;
mod error;
mod otlp;
#[allow(dead_code)]
mod schema;

#[path = ""]
mod opentelemetry {
    pub use crate::opentelemetry::proto::arrow::{
        ArrowPayload, ArrowPayloadType, BatchArrowRecords,
    };
    pub mod proto {
        #[allow(clippy::all)]
        #[path = "opentelemetry.proto.experimental.arrow.v1.rs"]
        pub mod arrow;
    }
}

pub use decode::decoder::Consumer;
