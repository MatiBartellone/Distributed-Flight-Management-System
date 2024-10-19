#[derive(Debug, PartialEq, Clone)]
pub struct FlightData {
    pub code: String,
    pub position: (f32, f32),
    pub altitude: f32,
    pub speed: f32,
}