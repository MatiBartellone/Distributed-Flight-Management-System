pub struct Airport {
    pub name: String,
    pub code: String,
    pub position: (f64, f64),
}

// Comunicacion con Cassandra o informacion de aeropuerto
impl Airport {
    pub fn new(name: String, code: String, position: (f64, f64)) -> Self {
        Airport {
            name,
            code,
            position,
        }
    }
}