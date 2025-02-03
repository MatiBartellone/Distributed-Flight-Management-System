FROM rust:latest

WORKDIR /node_handler

# Instalar dependencias necesarias
RUN apt-get update && apt-get install -y pkg-config libssl-dev libsqlite3-dev

# Copiar solo Cargo.toml para aprovechar la caché
COPY Cargo.toml ./


# Copiar el resto del código y compilar
COPY . .
RUN cargo build --release

# Asegurar permisos y ejecutar
RUN chmod +x ./target/release/node_handler
CMD ["./target/release/node_handler"]
