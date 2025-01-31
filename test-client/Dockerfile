FROM rust
WORKDIR /test-client
COPY . /test-client
RUN cargo build --release
CMD ["./target/release/test-client"]