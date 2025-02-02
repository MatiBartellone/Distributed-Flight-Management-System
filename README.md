# Taller de Programacion {Grupo}

## Integrantes

## Como usar

A continuacion se detallan los pasos para compilar y ejecutar el programa.

### Compilacion

### Como correr

## Como testear

# Final Docker

• Construir: docker-compose build --> para contenedor especifico --> sudo docker-compose build flight_app
• Iniciar: docker-compose up -d
--> para contenedor especifico --> sudo docker-compose up -d flight_app
--> para escalar el número de instancias --> sudo docker-compose up -d --scale node=3
--> para escalar el numero de instancias y solo ejecutar contenedores especificos --> docker-compose up --scale node=3 node seed_node
• Construir e Iniciar: docker-compose up --build
• Detener: docker-compose stop
• Eliminar: docker-compose down
