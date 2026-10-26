# syntax=docker/dockerfile:1
FROM rust:1.92.0-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p ocw

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 ocw && useradd --uid 10001 --gid 10001 --no-create-home ocw \
    && install -d -m 0700 -o ocw -g ocw /var/lib/ocw
COPY --from=build /src/target/release/ocw /usr/local/bin/ocw
USER 10001:10001
WORKDIR /var/lib/ocw
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/ocw"]
CMD ["--status"]
