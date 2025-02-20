# Distributed Flight Management System

Distributed Flight Management System is a robust and scalable distributed database engine implemented in Rust, designed to mimic Cassandra’s core functionality. The heart of the project is its Cassandra-compatible database, which features a multi-node architecture that supports key functionalities such as configurable consistency levels (strong and weak), hinted handoff, read repair, and replication strategies. This engine ensures data integrity and fault tolerance even when one or more nodes fail, and it supports dynamic reconfiguration—allowing nodes to be added or removed seamlessly while automatically redistributing partitions and data.

In addition to the database engine, the system includes a graphical flight application that displays a world map with airports and real-time positions of airplanes as stored in the database. A console-based flight simulator enables users to add new flights and update their positions dynamically over time, further demonstrating the system's ability to handle continuous data changes.

Moreover, the project offers full containerization support via Docker. With dedicated Dockerfiles and a docker-compose setup, you can easily deploy the entire database cluster—each node running in its own container—with comprehensive logging that outputs messages both to standard output and log files. This ensures that administrators can monitor inter-node communications and system performance in real time, making Distributed Flight Management System an all-encompassing solution for managing and visualizing distributed flight data.

## Contributors

- **Bartellone Matías**  
- **Maximoff Iván**  
- **Pacheco Thiago**  

## How to Use

The usage instructions for each component can be found in the report.

---

# Final Docker

## Basic Commands

### Build and Start
- **Build all containers:**  
  ```bash
  docker-compose build
  ```
- **Build a specific container:**  
  ```bash
  docker-compose build <service>
  ```
- **Start all containers in detached mode:**  
  ```bash
  docker-compose up -d
  ```
- **Start a specific container:**  
  ```bash
  docker-compose up -d <service>
  ```
- **Scale the number of instances of a service:**  
  ```bash
  docker-compose up -d --scale node=3
  ```
- **Scale the number of instances and run only specific containers:**  
  ```bash
  docker-compose up --scale node=3 <service>
  ```
- **Build and start in a single step:**  
  ```bash
  docker-compose up --build
  ```

### Stop and Remove
- **Stop the containers:**  
  ```bash
  docker-compose stop
  ```
- **Remove the containers:**  
  ```bash
  docker-compose down
  ```

## Logs

- **View logs up to the current moment:**  
  ```bash
  docker-compose logs
  ```
- **View real-time logs:**  
  ```bash
  docker-compose logs -f
  ```
- **View logs of a specific container:**  
  ```bash
  docker logs <container_id>
  ```
- **View real-time logs of a specific container:**  
  ```bash
  docker logs -f <container_id>
  ```

## Running Commands Inside Containers

To execute commands on specific nodes, the `.sh` scripts must be run inside the node.

1. **Grant execution permissions to the script:**  
   ```bash
   chmod +x /node/<script>.sh
   ```
   **Example:**  
   ```bash
   chmod +x /node/send_command.sh
   ```

2. **Run the script with the necessary arguments:**  
   ```bash
   ./send_command.sh <container_name> <command>
   ```

   **Other available commands:**
   ```bash
   ./exit_node.sh <container_name>
   ./pause_node.sh <container_name>
   ./resume_node.sh <container_name>
   ./state_node.sh <container_name>
   ```

## Connecting to Nodes from Outside Docker

To connect to the nodes from outside Docker, the ports are mapped:

- **First Seed Node:** Mapped to port `9090`, accessible via:
  ```bash
  localhost:9090
  ```
- **Scaled nodes:** Since the port is not fixed, you can check with:
  ```bash
  docker ps
  ```

