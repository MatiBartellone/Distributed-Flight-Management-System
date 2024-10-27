#[derive(Clone, PartialEq, Debug)]
pub enum FlightStatus {
    OnTime,
    Delayed,
    Cancelled,
    Arrived,
    Inactive
}

use FlightStatus::*;

impl Default for FlightStatus {
    fn default() -> Self {
        FlightStatus::Inactive
    }
}

impl FlightStatus {
    pub fn new(status: &str) -> Self {
        match status {
            "OnTime" => OnTime,
            "Delayed" => Delayed,
            "Canceled" => Cancelled,
            "Arrived" => Arrived,
            "Inactive" => Inactive,
            _ => FlightStatus::default()
        }
    }

    pub fn get_status(&self) -> String {
        match self {
            OnTime => "OnTime".to_string(),
            Delayed => "Delayed".to_string(),
            Cancelled => "Canceled".to_string(),
            Arrived => "Arrived".to_string(),
            Inactive => "Inactive".to_string()
        }
    }
}