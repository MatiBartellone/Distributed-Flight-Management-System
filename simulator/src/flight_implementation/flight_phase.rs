use super::flight::Flight;

pub const MAX_ALTITUDE: f64 = 10000.0;
pub const MIN_ALTITUDE: f64 = 500.0;
pub const MAX_SPEED: f32 = 850.0;
pub const MIN_SPEED: f32 = 200.0;

#[derive(Default, Clone, Debug)]
pub enum FlightPhaseType {
    #[default] Takeoff,
    Cruise,
    Descent,
}

impl FlightPhase for FlightPhaseType {
    fn update_altitude(&self, flight: &mut Flight, step: f32) {
        match self {
            FlightPhaseType::Takeoff => TakeoffState.update_altitude(flight, step),
            FlightPhaseType::Cruise => CruiseState.update_altitude(flight, step),
            FlightPhaseType::Descent => DescentState.update_altitude(flight, step),
        }
    }

    fn update_speed(&self, flight: &mut Flight, step: f32) {
        match self {
            FlightPhaseType::Takeoff => TakeoffState.update_speed(flight, step),
            FlightPhaseType::Cruise => CruiseState.update_speed(flight, step),
            FlightPhaseType::Descent => DescentState.update_speed(flight, step),
        }
    }

    fn update_fuel(&self, flight: &mut Flight, step: f32) {
        match self {
            FlightPhaseType::Takeoff => TakeoffState.update_fuel(flight, step),
            FlightPhaseType::Cruise => CruiseState.update_fuel(flight, step),
            FlightPhaseType::Descent => DescentState.update_fuel(flight, step),
        }
    }
}

pub trait FlightPhase {
    fn update_altitude(&self, flight: &mut Flight, step: f32);
    fn update_speed(&self, flight: &mut Flight, step: f32);
    fn update_fuel(&self, flight: &mut Flight, step: f32);
}

#[derive(Default, Debug)]
struct TakeoffState;
#[derive(Default, Debug)]
struct CruiseState;
#[derive(Default, Debug)]
struct DescentState;

/// At the beginning of the simulation, the flight is in the takeoff phase.
/// The altitude and speed increase until they reach the initial cruise values.
/// The fuel level decreases rapidly during this phase.
impl FlightPhase for TakeoffState {
    fn update_altitude(&self, flight: &mut Flight, step: f32) {
        if flight.get_altitude() < MAX_ALTITUDE {
            flight.tracking.altitude += 20.0 * step as f64;
            if flight.get_altitude() > MAX_ALTITUDE {
                flight.set_altitude(MAX_ALTITUDE);
            }
        }
    }

    fn update_speed(&self, flight: &mut Flight, step: f32) {
        if flight.get_speed() < MAX_SPEED {
            flight.tracking.speed += 15.0 * step;
            if flight.get_speed() > MAX_SPEED {
                flight.set_speed(MAX_SPEED);
            }
        }
    }

    fn update_fuel(&self, flight: &mut Flight, step: f32) {
        flight.tracking.fuel_level -= 0.1 * step;
    }
}

impl FlightPhase for CruiseState {
    fn update_altitude(&self, flight: &mut Flight, _step: f32) {
        if flight.get_altitude() < MAX_ALTITUDE {
            flight.tracking.altitude += 50.0;
        } else {
            let altitude_variation = random_in_range(-50.0, 50.0);
            let new_altitude = flight.get_altitude() + altitude_variation;
            flight.tracking.altitude = new_altitude.clamp(9000.0, MAX_ALTITUDE + 100.0);
        }
    }

    fn update_speed(&self, flight: &mut Flight, _step: f32) {
        if flight.get_speed() < MAX_SPEED {
            flight.tracking.speed += 5.0;
            if flight.get_speed() > MAX_SPEED {
                flight.set_speed(MAX_SPEED);
            }
        } else {
            let speed_variation = random_in_range(-5.0, 5.0) as f32;
            let new_speed = MAX_SPEED + speed_variation;
            flight.tracking.speed = new_speed.clamp(750.0, 950.0);
        }
    }

    fn update_fuel(&self, flight: &mut Flight, step: f32) {
        flight.tracking.fuel_level -= 0.02 * step;
    }
}

impl FlightPhase for DescentState {
    fn update_altitude(&self, flight: &mut Flight, step: f32) {
        flight.tracking.altitude -= 15.0 * step as f64;
        if flight.get_altitude() < MIN_ALTITUDE {
            flight.set_altitude(MIN_ALTITUDE);
        }
    }

    fn update_speed(&self, flight: &mut Flight, step: f32) {
        flight.tracking.speed -= 3.0 * step;
        if flight.get_speed() < MIN_SPEED {
            flight.set_speed(MIN_SPEED);
        }
    }

    fn update_fuel(&self, flight: &mut Flight, step: f32) {
        flight.tracking.fuel_level -= 0.01 * step; 
    }
}

use rand::Rng;

fn random_in_range(min: f64, max: f64) -> f64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(min..max)
}