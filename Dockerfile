FROM rust:1.75 AS builder

RUN mkdir /app
WORKDIR /app
COPY . /app

RUN cargo build --release

FROM debian:bookworm-slim AS app

RUN apt-get update && apt-get install -y libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/bin
COPY --from=builder /app/target/release/pmu .
COPY tests/test_data ./tests/test_data

# Environment variables
ENV PDC_HOST=localhost \
    PDC_PORT=8123 \
    PDC_IDCODE=8080 \
    SERVER_PORT=7734 \
    BUFFER_DURATION_SECS=120

EXPOSE 7734

CMD ["pmu", "server"]
