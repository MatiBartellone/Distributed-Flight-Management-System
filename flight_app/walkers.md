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