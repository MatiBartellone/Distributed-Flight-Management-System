FROM rust
WORKDIR /simulator
COPY . /simulator
RUN cargo build --release
CMD ["./target/release/simulator"]