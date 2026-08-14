FROM rust:1.97-bookworm AS build
WORKDIR /src
ARG CARGO_FEATURES
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --locked --release ${CARGO_FEATURES}

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 threadmark
COPY --from=build /src/target/release/threadmark /usr/local/bin/threadmark
USER threadmark
EXPOSE 8090
ENTRYPOINT ["threadmark"]
