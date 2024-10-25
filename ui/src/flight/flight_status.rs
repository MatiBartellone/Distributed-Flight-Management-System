#[derive(Clone, PartialEq)]
pub enum FlightStatus {
    OnTime,
    Delayed,
    Cancelled,
}

use FlightStatus::*;

impl Default for FlightStatus {
    fn default() -> Self {
        FlightStatus::OnTime
    }
}

impl FlightStatus {
    pub fn get_status(&self) -> String {
        match self {
            OnTime => "A tiempo".to_string(),
            Delayed => "Retrasado".to_string(),
            Cancelled => "Cancelado".to_string(),
        }
    }
}
