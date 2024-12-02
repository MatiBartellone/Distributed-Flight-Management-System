use std::fmt;

use FlightState::*;

#[derive(Clone, PartialEq, Debug, Default)]
pub enum FlightState {
    OnTime,
    Delayed,
    Cancelled,
    Arrived,
    #[default]
    Inactive,
}

impl FlightState {
    pub fn new(status: &str) -> Self {
        match status {
            "OnTime" => OnTime,
            "Delayed" => Delayed,
            "Canceled" => Cancelled,
            "Arrived" => Arrived,
            "Inactive" => Inactive,
            _ => FlightState::default(),
        }
    }
}
impl fmt::Display for FlightState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FlightState::OnTime => write!(f, "OnTime"),
            FlightState::Delayed => write!(f, "Delayed"),
            FlightState::Cancelled => write!(f, "Cancelled"),
            FlightState::Arrived => write!(f, "Arrived"),
            FlightState::Inactive => write!(f, "Inactive"),
        }
    }
}
