FROM rust:1.93.1-trixie AS builder

# protoc is required by tonic-prost-build
RUN apt-get update \
    && apt-get install -y protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

ENTRYPOINT ["bash", "-c"]
