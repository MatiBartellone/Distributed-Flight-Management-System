use super::repair_row::RepairRow;
use super::response_manager::ResponseManager;
use super::utils::to_hash_rows;
use crate::utils::types::node_ip::NodeIp;
use crate::{
    data_access::row::Row,
    parsers::{query_parser::query_parser, tokens::token::Token},
    query_delegation::query_delegator::QueryDelegator,
    utils::{constants::BEST, errors::Errors},
};
use std::collections::HashMap;

pub struct ReadRepair {
    response_manager: ResponseManager,
    repair_row: RepairRow,
}

impl ReadRepair {
    pub fn new(responses: &HashMap<NodeIp, Vec<u8>>) -> Result<Self, Errors> {
        let response_manager = ResponseManager::new(responses)?;

        Ok(Self {
            response_manager,
            repair_row: RepairRow::new(),
        })
    }

    pub fn get_response(&mut self) -> Result<Vec<u8>, Errors> {
        if self.response_manager.repair_unnecessary()? {
            return self.response_manager.get_first_response();
        }
        self.response_manager.get_better_response()?;
        self.set_repair_row()?;
        self.repair()?;
        self.response_manager.cast_to_protocol_row(BEST)
    }

    fn set_repair_row(&mut self) -> Result<(), Errors> {
        let (keyspace, table) = self.response_manager.get_keyspace_table(BEST)?;
        let pks = self.response_manager.get_pks_headers(BEST)?;
        self.repair_row
            .initializer(keyspace, table, pks.keys().cloned().collect());
        Ok(())
    }

    fn repair(&self) -> Result<(), Errors> {
        let better_response = self.response_manager.cast_to_protocol_row(BEST)?;
        for ip in self.response_manager.get_ips() {
            let response = self.response_manager.cast_to_protocol_row(&ip)?;
            if response != better_response {
                self.repair_node(&ip)?;
            }
        }
        Ok(())
    }

    fn repair_node(&self, ip: &str) -> Result<(), Errors> {
        let node_rows = self.response_manager.read_rows(ip)?;
        let mut best = to_hash_rows(self.response_manager.read_rows(BEST)?);
        self.process_existing_rows(ip, &node_rows, &mut best)?;
        self.process_remaining_rows(ip, &best)?;

        Ok(())
    }

    fn process_existing_rows(
        &self,
        ip: &str,
        node_rows: &[Row],
        best: &mut HashMap<Vec<String>, Row>,
    ) -> Result<(), Errors> {
        for node_row in node_rows {
            if let Some(best_row) = best.remove(&node_row.primary_key) {
                let (change_row, query) = self
                    .repair_row
                    .repair_row(best_row.clone(), node_row.clone())?;
                if change_row {
                    ReadRepair::send_reparation(query, ip)?;
                }
            }
        }
        Ok(())
    }

    fn process_remaining_rows(
        &self,
        ip: &str,
        best: &HashMap<Vec<String>, Row>,
    ) -> Result<(), Errors> {
        for row in best.values() {
            if !row.is_deleted() {
                let query = self.repair_row.create_insert(row)?;
                ReadRepair::send_reparation(query, ip)?;
            }
        }
        Ok(())
    }

    fn send_reparation(query: Vec<Token>, ip: &str) -> Result<(), Errors> {
        let query_parsed = query_parser(query)?;
        let node_ip = NodeIp::new_from_single_string(ip)?;
        QueryDelegator::send_to_node(node_ip, query_parsed)?;
        Ok(())
    }
}
