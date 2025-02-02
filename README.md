# Taller de Programacion {Grupo}

## Integrantes

## Como usar

A continuacion se detallan los pasos para compilar y ejecutar el programa.

### Compilacion

### Como correr

## Como testear

# Final Docker

• Construir: docker-compose build --> para contenedor especifico --> sudo docker-compose build <service> ...
• Iniciar: docker-compose up -d
--> para contenedor especifico --> sudo docker-compose up -d <service> ...
--> para escalar el número de instancias --> sudo docker-compose up -d --scale node=3
--> para escalar el numero de instancias y solo ejecutar contenedores especificos --> docker-compose up --scale node=3 <service> ...
• Construir e Iniciar: docker-compose up --build
• Detener: docker-compose stop
• Eliminar: docker-compose down
• Ver logs hasta el momento: docker-compose logs
• Ver logs de todos en tiempo real: docker-compose logs -f
• Ver logs hasta el momento: docker logs <container_id>
• Ver logs de todos en tiempo real de un contenedor en especifico: docker logs -f <container_id>
• Ejecutar con entrada de la terminal: docker-compose run --rm <service>
