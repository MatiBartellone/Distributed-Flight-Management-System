pub fn get_airports() -> Vec<String> {
    vec![
        "EZE".to_string(),
        "JFK".to_string(),
        "SCL".to_string(),
        "MIA".to_string(),
        "DFW".to_string(),
        "GRU".to_string(),
        "MAD".to_string(),
        "CDG".to_string(),
        "LAX".to_string(),
        "AMS".to_string(),
        "NRT".to_string(),
        "LHR".to_string(),
        "FRA".to_string(),
        "SYD".to_string(),
        "SFO".to_string(),
    ]
}

pub fn get_airport_coordinates(airport: &Option<String>) -> (f64, f64) {
    let Some(airport) = airport else { return (0.0, 0.0) }; 
    match airport.as_str() {
        "JFK" => (-73.7781, 40.6413),  // JFK, Nueva York
        "LAX" => (-118.4085, 33.9416), // LAX, Los Ángeles
        "EZE" => (-58.5358, -34.8222), // EZE, Buenos Aires (Ezeiza)
        "CDG" => (2.55, 49.0097),      // CDG, París Charles de Gaulle
        "LHR" => (-0.4543, 51.4700),   // LHR, Londres Heathrow
        "NRT" => (140.3929, 35.7735),  // NRT, Tokio Narita
        "FRA" => (8.5706, 50.0333),    // FRA, Frankfurt
        "SYD" => (151.1772, -33.9399), // SYD, Sídney Kingsford Smith
        "SCL" => (-70.7853, -33.3929), // SCL, Santiago de Chile
        "MIA" => (-80.2906, 25.7957),  // MIA, Miami
        "DFW" => (-97.0379, 32.8968),  // DFW, Dallas/Fort Worth
        "GRU" => (-46.6506, -23.4253),  // GRU, São Paulo-Guarulhos
        "MAD" => (-3.5705, 40.4719),    // MAD, Madrid
        "AMS" => (4.7600, 52.3081),     // AMS, Ámsterdam
        "SFO" => (-122.4194, 37.6213),  // SFO, San Francisco
        _ => (0.0, 0.0)
    }
}