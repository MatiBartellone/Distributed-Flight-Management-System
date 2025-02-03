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
Entrar

Para ejecutar comandos en nodos especificos se deben ejecutar los archivos .sh dentro del nodo.
Primero se les tiene que dat permiso de ejecucion con chmod +x /node/<script>.sh --> ej: chmod +x /node/send_command.sh
luego se ejecuta corriendolo enviandole los argumentos necesarios, esta es la lista de comandos:
./send_command.sh <container_name> <command>
./exit_node.sh <container_name>
./pause_node.sh <container_name>
./resume_node.sh <container_name>
./state_node.sh <container_name>

Para poder conectarte a los nodos desde fuera de docker se mapean los puertos, de tal manera que se puede conectar con localhost:<puerto_mapeado>
Para first_seed_node el puerto mapeado es 9090.
Para los nodos, al ser escalable el puerto no es fijo. Asi que al correrlos se usa 'docker ps' para ver el puerto al que fue mapeado.
