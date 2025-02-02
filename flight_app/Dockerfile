FROM rust
WORKDIR /flight_app
COPY . /flight_app
RUN cargo build --release
CMD ["./target/release/flight_app"]