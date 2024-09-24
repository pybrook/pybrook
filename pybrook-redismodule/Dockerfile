FROM rust:1.80.1-slim-bullseye AS builder

RUN apt-get update -y && apt-get install -y libclang-dev
COPY Cargo.lock Cargo.toml .
COPY src/ ./src/
RUN cargo build --release

FROM redis:7.4.0

COPY --from=builder target/release/libpybrook_redis.so /usr/lib/redis/modules/pybrook.so

CMD ["redis-server", "--loadmodule", "/usr/lib/redis/modules/pybrook.so"]