use std::collections::HashMap;

pub struct HydisTcpClient {
    // vector clock
    vector_clock: HashMap<String, u64>,
}

impl HydisTcpClient {
    pub fn new() -> Self {
        Self {
            vector_clock: HashMap::new(),
        }
    }

    pub fn set_vc(&mut self, map: HashMap<String, u64>) {
        self.vector_clock = map;
    }

    pub fn get_vc(&self) -> HashMap<String, u64> {
        self.vector_clock.clone()
    }
}
