#!/bin/bash
set -e
IP_ADDRESS=$(hostname -i)  # Obtiene la IP del contenedor

# Modificar el archivo config.yaml con las variables de entorno y el DNS
cat <<EOF > src/config.yaml
ip:
  ip: "$IP_ADDRESS"
  port: 9090
seed_ip:
  ip: "192.168.100.2"
  port: 9090
is_first: ${IS_FIRST}
is_seed: ${IS_SEED}
EOF
./target/release/node default