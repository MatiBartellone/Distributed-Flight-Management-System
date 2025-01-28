use super::{cluster::Cluster, node::Node};
use crate::meta_data::nodes::node::State;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::{deserialize_from_slice, write_all_to_file};
use crate::utils::types::node_ip::NodeIp;
use crate::{
    meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess,
    utils::{constants::KEYSPACE_METADATA_PATH, errors::Errors},
};
use murmur3::murmur3_32;
use std::{fs::File, io::Cursor};
use std::collections::HashSet;

#[derive(Debug)]
pub struct NodesMetaDataAccess;

impl NodesMetaDataAccess {
    fn read_cluster(path: &str) -> Result<Cluster, Errors> {
        let content = std::fs::read_to_string(path)
            .map_err(|_| ServerError(String::from("Error leyendo el archivo")))?;
        let cluster: Cluster = deserialize_from_slice(content.as_bytes())?;
        Ok(cluster)
    }

    pub fn get_full_nodes_list(&self, path: &str) -> Result<Vec<Node>, Errors> {
        let cluster = Self::read_cluster(path)?;
        let mut nodes_list = Vec::new();
        for node in cluster.get_other_nodes() {
            nodes_list.push(Node::new_from_node(node))
        }
        nodes_list.push(Node::new_from_node(cluster.get_own_node()));
        Ok(nodes_list)
    }

    pub fn get_cluster(&self, path: &str) -> Result<Cluster, Errors> {
        Self::read_cluster(path)
    }

    pub fn write_cluster(path: &str, cluster: &Cluster) -> Result<(), Errors> {
        let mut file = File::create(path)
            .map_err(|_| ServerError("Failed to open file for writing".to_string()))?;
        let serialized = serde_json::to_vec(cluster).map_err(|e| ServerError(e.to_string()))?;
        write_all_to_file(&mut file, serialized.as_slice())?;
        Ok(())
    }

    pub fn set_new_cluster(&self, path: &str, cluster: &Cluster) -> Result<(), Errors> {
        Self::write_cluster(path, cluster)
    }

    pub fn get_own_ip(&self, path: &str) -> Result<NodeIp, Errors> {
        let cluster = Self::read_cluster(path)?;
        Ok(NodeIp::new_from_ip(cluster.get_own_ip()))
    }

    pub fn set_state(&self, path: &str, ip: &NodeIp, state: State) -> Result<(), Errors> {
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        let mut nodes_list = Vec::new();
        for node in cluster.get_other_nodes() {
            if node.get_ip() != ip {
                nodes_list.push(Node::new_from_node(node))
            } else {
                let mut inactive = Node::new_from_node(node);
                inactive.set_state(&state);
                inactive.update_timestamp();
                nodes_list.push(inactive);
            }
        }
        let new_cluster = Cluster::new(Node::new_from_node(cluster.get_own_node()), nodes_list);
        Self::write_cluster(path, &new_cluster)?;
        Ok(())
    }

    pub fn set_inactive(&self, path: &str, ip: &NodeIp) -> Result<(), Errors> {
        self.set_state(path, ip, State::Inactive)
    }

    pub fn set_booting(&self, path: &str, ip: &NodeIp) -> Result<(), Errors> {
        self.set_state(path, ip, State::Booting)
    }

    pub fn set_active(&self, path: &str, ip: &NodeIp) -> Result<(), Errors> {
        self.set_state(path, ip, State::Active)
    }
    pub fn set_stand_by(&self, path: &str, ip: &NodeIp) -> Result<(), Errors> {
        self.set_state(path, ip, State::StandBy)
    }

    pub fn set_shutting_down(&self, path: &str, ip: &NodeIp) -> Result<(), Errors> {
        self.set_state(path, ip, State::ShuttingDown)
    }

    pub fn set_recovering(&self, path: &str, ip: &NodeIp) -> Result<(), Errors> {
        self.set_state(path, ip, State::Recovering)
    }

    pub fn set_own_state(&self, path: &str, state: State) -> Result<(), Errors> {
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        let mut new_node = Node::new_from_node(cluster.get_own_node());
        new_node.set_state(&state);
        new_node.update_timestamp();
        let mut nodes_list = Vec::new();
        for node in cluster.get_other_nodes() {
            nodes_list.push(Node::new_from_node(node))
        }
        let new_cluster = Cluster::new(new_node, nodes_list);
        Self::write_cluster(path, &new_cluster)?;
        Ok(())
    }

    pub fn set_own_node_to_shutting_down(&self, path: &str) -> Result<(), Errors> {
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        let mut new_node = Node::new_from_node(cluster.get_own_node());
        new_node.set_shutting_down();
        new_node.set_nonexistent_range();
        new_node.set_pos(0);
        new_node.update_timestamp();
        let mut nodes_list = Vec::new();
        for node in cluster.get_other_nodes() {
            nodes_list.push(Node::new_from_node(node))
        }
        let new_cluster = Cluster::new(new_node, nodes_list);
        Self::write_cluster(path, &new_cluster)?;
        Ok(())
    }

    pub fn set_own_node_active(&self, path: &str) -> Result<(), Errors> {
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        let mut new_node = Node::new_from_node(cluster.get_own_node());
        new_node.set_active();
        new_node.update_timestamp();
        let mut nodes_list = Vec::new();
        for node in cluster.get_other_nodes() {
            nodes_list.push(Node::new_from_node(node))
        }
        let new_cluster = Cluster::new(new_node, nodes_list);
        Self::write_cluster(path, &new_cluster)?;
        Ok(())
    }

    pub fn get_partition_full_ips(
        &self,
        path: &str,
        primary_key: &Option<Vec<String>>,
        keyspace: String,
    ) -> Result<Vec<NodeIp>, Errors> {
        let cluster = Self::read_cluster(path)?;
        if let Some(primary_key) = primary_key {
            let hashing_key = hash_string_murmur3(&primary_key.join(""));
            Self::get_range_based_partitions(hashing_key, &cluster, keyspace)
        } else {
            cluster.get_all_ips()
        }
    }

    // rangos mejores distribuidos pero mayor cambio al agregar o sacar nodos
    #[allow(dead_code)]
    fn get_mod_based_partitions(hashing_key: usize, cluster: &Cluster, keyspace: String) -> Result<Vec<NodeIp>, Errors> {
        let pos = hashing_key % cluster.len_nodes() + 1;
        let keyspace_metadata = KeyspaceMetaDataAccess {};
        let replication =
            keyspace_metadata.get_replication(KEYSPACE_METADATA_PATH.to_owned(), &keyspace)?;
        cluster.get_nodes(pos, replication)
    }

    // rangos distribuidos linealmente pero menor cambio al agregar o sacar nodos
    #[allow(dead_code)]
    fn get_range_based_partitions(hashing_key: usize, cluster: &Cluster, keyspace: String) -> Result<Vec<NodeIp>, Errors> {
        let pos = cluster.get_node_pos_by_range(hashing_key)?;
        let keyspace_metadata = KeyspaceMetaDataAccess {};
        let replication =
            keyspace_metadata.get_replication(KEYSPACE_METADATA_PATH.to_owned(), &keyspace)?;
        cluster.get_nodes(pos, replication)
    }

    pub fn append_new_node(&self, path: &str, new_node: Node) -> Result<(), Errors> {
        let mut cluster = NodesMetaDataAccess::read_cluster(path)?;
        cluster.append_new_node(new_node);
        Self::write_cluster(path, &cluster)?;
        Ok(())
    }

    pub fn get_nodes_quantity(&self, path: &str) -> Result<usize, Errors> {
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        Ok(cluster.len_nodes())
    }

    pub fn get_recovering_nodes(&self, path: &str) -> Result<Vec<NodeIp>, Errors> {
        let mut nodes = Vec::new();
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        for node in cluster.get_other_nodes() {
            if node.state == State::Recovering {
                nodes.push(NodeIp::new_from_ip(node.get_ip()));
            }
        }
        Ok(nodes)
    }

    pub fn update_ranges(&self, path: &str) -> Result<(), Errors> {
        self.update_positions(path)?;
        let nodes_quantity = self
            .get_full_nodes_list(path)?
            .iter()
            .filter(|node| node.state != State::ShuttingDown)
            .count();
        let mut own_node = Node::new_from_node(Self::read_cluster(path)?.get_own_node());
        if own_node.state == State::ShuttingDown {
            own_node.set_nonexistent_range()
        } else {
            own_node.set_range_by_pos(nodes_quantity);
        }
        let mut other_nodes = Vec::new();
        for node in Self::read_cluster(path)?.get_other_nodes() {
            let mut new_node = Node::new_from_node(node);
            if node.state == State::ShuttingDown {
                new_node.set_nonexistent_range()
            } else {
                new_node.set_range_by_pos(nodes_quantity);
            }
            other_nodes.push(new_node);
        }
        Self::write_cluster(path, &Cluster::new(own_node, other_nodes))
    }

    fn update_positions(&self, path: &str) -> Result<(), Errors> {
        let missing_position = self.get_missing_position(path)?;
        if missing_position == 0 {
            return Ok(());
        }

        let cluster = Self::read_cluster(path)?;
        let (node, other_nodes) = (cluster.get_own_node(), cluster.get_other_nodes());
        let mut new_nodes = Vec::new();
        for node in other_nodes {
            let mut new_node = Node::new_from_node(node);
            if node.position > missing_position {
                new_node.set_pos(node.position - 1);
            }
            new_nodes.push(new_node);
        }
        let mut new_node = Node::new_from_node(node);
        if node.position > missing_position {
            new_node.set_pos(node.position - 1);
        }
        Self::write_cluster(path, &Cluster::new(new_node, new_nodes))
    }

    fn get_missing_position(&self, path: &str) -> Result<usize, Errors> {
        let node_quantity = self.get_nodes_quantity(path)?;
        let mut positions: HashSet<usize> = HashSet::new();
        for node in &self.get_full_nodes_list(path)? {
            if node.position > 0 { // Only consider active nodes
                positions.insert(node.position);
            }
        }
        let mut missing_position = 0;
        for pos in 1..=node_quantity {
            if !positions.contains(&pos) {
                missing_position = pos;
                break;
            }
        }
        Ok(missing_position)
    }
}

fn hash_string_murmur3(input: &str) -> usize {
    let mut buffer = Cursor::new(input.as_bytes());
    let hash = murmur3_32(&mut buffer, 0).expect("Unable to compute hash");
    hash as usize
}
