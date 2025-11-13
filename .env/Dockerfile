# Use the latest Ubuntu LTS as the base image
FROM ubuntu:24.04

# Set environment variables to avoid user interaction during package installation,
# and add Go binaries to PATH
ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/root/go/bin:${PATH}"

# Update and install common dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gcc \
        gcc-multilib \
        protobuf-compiler \
        libprotobuf-dev \
        git \
        golang-go

# install rust and set specific version
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup install 1.89
RUN rustup default 1.89
ENV RUST_BACKTRACE=1

# install grpcurl (runs test grpc calls)
RUN go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.3

# setup rustDFS config
RUN mkdir -p /etc/rustdfs
COPY rdfsconf.toml /etc/rustdfs/rdfsconf.toml
