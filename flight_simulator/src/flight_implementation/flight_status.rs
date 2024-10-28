use FlightStatus::*;

#[derive(PartialEq, Debug, Default)]
pub enum FlightStatus {
    OnTime,
    Delayed,
    Cancelled,
    Arrived,
    #[default]
    Inactive
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