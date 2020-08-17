use super::RepcService;

#[derive(Clone)]
pub struct RepcServicePeer {
    service: RepcService,
}

impl RepcServicePeer {
    pub fn new(service: RepcService) -> Self {
        Self { service }
    }
}

