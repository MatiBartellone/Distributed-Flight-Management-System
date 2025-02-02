#!/bin/bash
set -e

IP_ADDRESS=$(hostname -i)  # Obtiene la IP del contenedor

# Modificar el archivo config.yaml con las variables de entorno y el DNS
cat <<EOF > src/config.yaml
ip:
  ip: "$IP_ADDRESS"
  port: 9090
seed_ip:
  ip: "127.0.0.2"
  port: 9090
is_first: ${IS_FIRST}
is_seed: ${IS_SEED}
EOF

echo "=== Configuración final (src/config.yaml) ==="
cat src/config.yaml
echo ""

./target/release/node default