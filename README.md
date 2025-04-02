# This repository has been deprecated. Furthuer development will continue on [opentelemetry/otel-arrow](https://github.com/open-telemetry/otel-arrow)



# OTEL Arrow Protocol Implementation in Rust

The non-official implementation for [OTEL Arrow protocol](https://github.com/open-telemetry/otel-arrow), written in Rust.

- Decoding Arrow IPC record batches to Opentelemetry data structures.
    - 🚧 Metrics
    - [ ] Logs
    - [ ] Traces
- Encoding Opentelemetry data structures to Arrow IPC record batches.
    - [ ] Metrics
    - [ ] Logs
    - [ ] Traces

## Build

```bash
git clone https://github.com/v0y4g3r/otel-arrow-rust.git
cd otel-arrow-rust && git submodule update --init --recursive
cargo build --release
```
