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
# Present and owned by the runtime user so a named volume mounted here inherits
# that ownership. Without it the single-node shape cannot create its database
# file or blob directory.
RUN mkdir -p /data && chown threadmark:threadmark /data
USER threadmark
VOLUME ["/data"]
EXPOSE 8090
ENTRYPOINT ["threadmark"]
