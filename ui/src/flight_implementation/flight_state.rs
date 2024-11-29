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

    pub fn to_string(&self) -> String {
        match self {
            OnTime => "OnTime".to_string(),
            Delayed => "Delayed".to_string(),
            Cancelled => "Canceled".to_string(),
            Arrived => "Arrived".to_string(),
            Inactive => "Inactive".to_string(),
        }
    }
}