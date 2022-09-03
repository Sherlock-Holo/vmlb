FROM rust as builder

RUN apt update && apt install -y protobuf-compiler

COPY . /vmlb

WORKDIR /vmlb

RUN cargo build --release

FROM gcr.io/distroless/cc

COPY --from=builder /vmlb/target/release/vmlb /vmlb

ENTRYPOINT ["/vmlb"]
