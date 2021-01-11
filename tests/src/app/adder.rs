pub mod pb {
    tonic::include_proto!("adder");
}

#[derive(Default, Clone)]
pub struct AdderState {
    n: u32,
}

impl pb::adder_server::Adder for AdderState {
    fn add(
        &mut self,
        req: pb::AddRequest,
    ) -> Result<tonic::Response<pb::AddResponse>, tonic::Status> {
        self.n += req.i;
        Ok(tonic::Response::new(pb::AddResponse { n: self.n }))
    }
}
