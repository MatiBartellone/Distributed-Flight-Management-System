APP FLIGHT

Mapa por aeropuerto ya se que sea de llegada o partida.
Estado vuelos -> On-Time o Delayed -> Strong Consistency.
Seguimiento de vuelos -> code, posicion {latitude, longitude}, altitude, speed.
Editar estado de vuelo On-Time a Delayed o al revez.

SIMULADOR DE VUELOS

Multithread que avanza el viaje de todos los vuelos. 
Ecuacion para actualizar los datos de vuelo:

WALKERS

Map
-> Tiles: imagen mapa mundi
-> MapMemory: metadata del estado del mapa
-> Position: {longitude, latitude}

Plugins
-> Objetos funcionales adentro del mapa (aviones)
-> impl Plugin for Flight
    fn run(&mut self, response: &Response, painter: Painter, projector: &Projector)
    -> Response: Whenever something gets added to a [`Ui`], a [`Response`] object is returned.
    -> Painter: se crea en la ui
    -> Proyector: 
-> Texturas: 




CREATE TABLE airports (
    code TEXT PRIMARY KEY,
    name TEXT,
    position_x FLOAT,
    position_y FLOAT
);

CREATE TABLE flights_by_airport (
    airport_code TEXT,
    date DATE,
    flight_code TEXT,
    type TEXT,
    PRIMARY KEY ((airport_code, date), flight_code)
);

CREATE TABLE flight_info (
    flight_code TEXT PRIMARY KEY,
    departure_airport TEXT,
    arrival_airport TEXT,
    departure_time TEXT,
    arrival_time TEXT,
    status TEXT,
    position_x FLOAT,
    position_y FLOAT
    altitude FLOAT,
    speed FLOAT,
    fuel_level FLOAT
);
