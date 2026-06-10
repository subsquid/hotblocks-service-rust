FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.89-slim-bookworm AS chef
WORKDIR /app

FROM --platform=$BUILDPLATFORM chef AS planner

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json


FROM --platform=$BUILDPLATFORM chef AS builder

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install pkg-config libssl-dev build-essential

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml .
COPY Cargo.lock .
COPY crates ./crates

RUN cargo build --release -p evm-data-service


FROM debian:bookworm-slim

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install curl ca-certificates net-tools

WORKDIR /run

COPY --from=builder /app/target/release/evm-data-service /usr/local/bin/evm-data-service

EXPOSE 3000

ENTRYPOINT ["evm-data-service"]
