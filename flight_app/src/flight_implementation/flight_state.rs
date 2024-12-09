use std::fmt;

use egui::Color32;
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

    pub fn get_color(&self) -> Color32 {
        match self {
            FlightState::OnTime => Color32::GREEN,
            FlightState::Delayed => Color32::YELLOW,
            FlightState::Cancelled => Color32::RED,
            FlightState::Arrived => Color32::BLUE,
            FlightState::Inactive => Color32::GRAY, 
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