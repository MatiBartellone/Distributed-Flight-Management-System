FROM rust
WORKDIR /node
COPY . /node
RUN cargo build --release

COPY *.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
