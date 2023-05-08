FROM rust:alpine as builder

RUN apk update && apk add protobuf libc-dev

WORKDIR /app
COPY Cargo.toml .
COPY src src

RUN cargo install --path .
RUN cargo clean

# minimal image

FROM alpine
COPY --from=builder /usr/local/cargo/bin/osm2parquet /usr/local/bin/osm2parquet

CMD ["osm2parquet"]
