FROM rust
WORKDIR /node
COPY . /node
RUN cargo build --release
CMD ["./target/release/node"]