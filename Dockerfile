FROM rust:alpine as builder

RUN apk update && apk add protobuf libc-dev

WORKDIR /app
COPY Cargo.toml .
COPY src src

RUN cargo install --path .
RUN cargo clean

# minimal image

FROM alpine
WORKDIR /app
COPY --from=builder /usr/local/cargo/bin/osm2parquet /usr/local/bin/osm2parquet
COPY --from=builder /app/src/osm2parquet.sh /app/osm2parquet.sh

CMD ["osm2parquet"]
