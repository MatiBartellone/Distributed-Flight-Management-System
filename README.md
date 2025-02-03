# Taller de Programación {Grupo}

## Integrantes

- **Bartellone Matías**  
- **Maximoff Iván**  
- **Pacheco Thiago**  

## Cómo Usar

Las instrucciones de uso para cada componente se encuentran dentro del informe.

---

# Final Docker

## Comandos Básicos

### Construcción e Inicio
- **Construir todos los contenedores:**  
  ```bash
  docker-compose build
  ```
- **Construir un contenedor específico:**  
  ```bash
  docker-compose build <service>
  ```
- **Iniciar todos los contenedores en modo desapegado:**  
  ```bash
  docker-compose up -d
  ```
- **Iniciar un contenedor específico:**  
  ```bash
  docker-compose up -d <service>
  ```
- **Escalar el número de instancias de un servicio:**  
  ```bash
  docker-compose up -d --scale node=3
  ```
- **Escalar el número de instancias y ejecutar solo contenedores específicos:**  
  ```bash
  docker-compose up --scale node=3 <service>
  ```
- **Construir e iniciar en un solo paso:**  
  ```bash
  docker-compose up --build
  ```

### Detención y Eliminación
- **Detener los contenedores:**  
  ```bash
  docker-compose stop
  ```
- **Eliminar los contenedores:**  
  ```bash
  docker-compose down
  ```

## Logs

- **Ver logs hasta el momento:**  
  ```bash
  docker-compose logs
  ```
- **Ver logs en tiempo real:**  
  ```bash
  docker-compose logs -f
  ```
- **Ver logs de un contenedor específico:**  
  ```bash
  docker logs <container_id>
  ```
- **Ver logs en tiempo real de un contenedor específico:**  
  ```bash
  docker logs -f <container_id>
  ```

## Ejecución de Comandos en Contenedores

Para ejecutar comandos en nodos específicos, se deben ejecutar los scripts `.sh` dentro del nodo.

1. **Dar permisos de ejecución al script:**  
   ```bash
   chmod +x /node/<script>.sh
   ```
   **Ejemplo:**  
   ```bash
   chmod +x /node/send_command.sh
   ```

2. **Ejecutar el script con los argumentos necesarios:**  
   ```bash
   ./send_command.sh <container_name> <command>
   ```
   
   **Otros comandos disponibles:**
   ```bash
   ./exit_node.sh <container_name>
   ./pause_node.sh <container_name>
   ./resume_node.sh <container_name>
   ./state_node.sh <container_name>
   ```

## Conexión a los Nodos Desde Fuera de Docker

Para conectarse a los nodos desde fuera de Docker, los puertos se encuentran mapeados:

- **First Seed Node:** Mapeado en el puerto `9090`, se puede acceder con:
  ```bash
  localhost:9090
  ```
- **Nodos escalados:** Como el puerto no es fijo, se puede verificar con:
  ```bash
  docker ps
  ```

