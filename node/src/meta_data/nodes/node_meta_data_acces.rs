use super::{cluster::Cluster, node::Node};
use crate::meta_data::nodes::node::State;
use crate::utils::config_constants::SHUTTING_DOWN_TIMEOUT_SECS;
use crate::utils::constants::NODES_METADATA_PATH;
use crate::utils::errors::Errors::ServerError;
use crate::utils::functions::{deserialize_from_slice, write_all_to_file};
use crate::utils::types::node_ip::NodeIp;
use crate::{
    meta_data::keyspaces::keyspace_meta_data_acces::KeyspaceMetaDataAccess,
    utils::{constants::KEYSPACE_METADATA_PATH, errors::Errors},
};
use murmur3::murmur3_32;
use std::collections::HashSet;
use std::{fs::File, io::Cursor};

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
    fn get_mod_based_partitions(
        hashing_key: usize,
        cluster: &Cluster,
        keyspace: String,
    ) -> Result<Vec<NodeIp>, Errors> {
        let pos = hashing_key % cluster.len_nodes() + 1;
        let keyspace_metadata = KeyspaceMetaDataAccess {};
        let replication =
            keyspace_metadata.get_replication(KEYSPACE_METADATA_PATH.to_owned(), &keyspace)?;
        cluster.get_nodes(pos, replication)
    }

    // rangos distribuidos linealmente pero menor cambio al agregar o sacar nodos
    #[allow(dead_code)]
    fn get_range_based_partitions(
        hashing_key: usize,
        cluster: &Cluster,
        keyspace: String,
    ) -> Result<Vec<NodeIp>, Errors> {
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

    pub fn get_booting_ips(&self, path: &str) -> Result<Vec<NodeIp>, Errors> {
        let mut nodes = Vec::new();
        let cluster = NodesMetaDataAccess::read_cluster(path)?;
        for node in cluster.get_other_nodes() {
            if node.state == State::Booting {
                nodes.push(NodeIp::new_from_ip(node.get_ip()));
            }
        }
        Ok(nodes)
    }

    pub fn update_ranges(&self, path: &str) -> Result<(), Errors> {
        let cluster = Self::read_cluster(path)?;
        let mut own_node = Node::new_from_node(cluster.get_own_node());
        let mut updated_other_nodes = self.get_new_node_vec(cluster.get_other_nodes());

        let active_node_count = self.calculate_active_node_count(&own_node, &cluster)?;

        if let Some(missing_position) =
            self.get_missing_position_from_nodes(&own_node, &updated_other_nodes)?
        {
            self.adjust_node_positions(&mut own_node, &mut updated_other_nodes, missing_position);
        }

        self.update_node_ranges(&mut own_node, &mut updated_other_nodes, active_node_count);

        Self::write_cluster(path, &Cluster::new(own_node, updated_other_nodes))?;
        Ok(())
    }

    fn get_new_node_vec(&self, nodes: &Vec<Node>) -> Vec<Node> {
        let mut new_nodes = Vec::new();
        for node in nodes {
            new_nodes.push(Node::new_from_node(node))
        }
        new_nodes
    }

    fn calculate_active_node_count(
        &self,
        own_node: &Node,
        cluster: &Cluster,
    ) -> Result<usize, Errors> {
        let active_node_count = cluster
            .get_other_nodes()
            .iter()
            .filter(|node| node.state != State::ShuttingDown)
            .count();

        Ok(active_node_count
            + (if own_node.state != State::ShuttingDown {
                1
            } else {
                0
            }))
    }

    fn update_node_ranges(
        &self,
        own_node: &mut Node,
        updated_other_nodes: &mut Vec<Node>,
        active_node_count: usize,
    ) {
        if own_node.state == State::ShuttingDown {
            own_node.set_nonexistent_range();
        } else {
            own_node.set_range_by_pos(active_node_count);
        }

        for node in updated_other_nodes {
            if node.state == State::ShuttingDown {
                node.set_nonexistent_range();
            } else {
                node.set_range_by_pos(active_node_count);
            }
        }
    }

    fn adjust_node_positions(
        &self,
        own_node: &mut Node,
        updated_other_nodes: &mut [Node],
        missing_position: usize,
    ) {
        for node in updated_other_nodes.iter_mut() {
            if node.position > missing_position {
                node.set_pos(node.position - 1);
            }
        }
        if own_node.position > missing_position {
            own_node.set_pos(own_node.position - 1);
        }
    }

    fn get_missing_position_from_nodes(
        &self,
        own_node: &Node,
        other_nodes: &[Node],
    ) -> Result<Option<usize>, Errors> {
        let mut positions: HashSet<usize> = HashSet::new();
        if own_node.position > 0 {
            positions.insert(own_node.position);
        }
        for node in other_nodes {
            if node.position > 0 {
                positions.insert(node.position);
            }
        }
        let node_quantity = positions.len();
        for pos in 1..=node_quantity {
            if !positions.contains(&pos) {
                return Ok(Some(pos));
            }
        }
        Ok(None) // No missing position.
    }

    pub fn check_for_perished_shutting_down_nodes(&self) -> Result<(), Errors> {
        for node in Self::read_cluster(NODES_METADATA_PATH)?.get_other_nodes() {
            if node.state == State::ShuttingDown
                && node
                    .get_timestamp()
                    .has_perished_seconds(SHUTTING_DOWN_TIMEOUT_SECS)
            {
                self.eliminate_perished_shutting_down_nodes()?
            }
        }
        Ok(())
    }

    fn eliminate_perished_shutting_down_nodes(&self) -> Result<(), Errors> {
        let cluster = Self::read_cluster(NODES_METADATA_PATH)?;
        let mut nodes_list = Vec::new();
        for node in cluster.get_other_nodes() {
            if node.state != State::ShuttingDown
                || !node
                    .get_timestamp()
                    .has_perished_seconds(SHUTTING_DOWN_TIMEOUT_SECS)
            {
                nodes_list.push(Node::new_from_node(node))
            }
        }
        let new_cluster = Cluster::new(Node::new_from_node(cluster.get_own_node()), nodes_list);
        Self::write_cluster(NODES_METADATA_PATH, &new_cluster)
    }
}

fn hash_string_murmur3(input: &str) -> usize {
    let mut buffer = Cursor::new(input.as_bytes());
    let hash = murmur3_32(&mut buffer, 0).expect("Unable to compute hash");
    hash as usize
}
