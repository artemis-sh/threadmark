FROM rust:1.97-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --locked --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 threadmark
COPY --from=build /src/target/release/threadmark /usr/local/bin/threadmark
USER threadmark
EXPOSE 8090
ENTRYPOINT ["threadmark"]
