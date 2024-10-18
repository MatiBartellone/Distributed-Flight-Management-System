

#[derive(Debug)]
pub struct NodesMetaDataAccess {
    file: Arc<Mutex<File>>, // Compartir el Mutex
}

impl NodesMetaDataAccess {
    pub fn new(path: &str, node: Node) -> Result<Self, Errors> {
        let file = OpenOptions::new()
        .read(true)  // Permitir lectura
        .write(true) // Permitir escritura
        .create(true) // Crear el archivo si no existe
        .open(path)
        .map_err(|_| Errors::ServerError("Unable to open or create file".to_string()))?;
        let mutex = Arc::new(Mutex::new(file));
        Ok(Self { file: mutex })
    }


    fn lock_and_extract_keyspaces(
        &self,
    ) -> Result<(MutexGuard<File>, HashMap<String, Keyspace>), Errors> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| Errors::ServerError("Failed to acquire lock".to_string()))?;
        let keyspaces = self.extract_hash_from_json(&mut file)?;
        Ok((file, keyspaces))
    }
}