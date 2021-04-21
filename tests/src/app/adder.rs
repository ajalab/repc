pub mod pb {
    tonic::include_proto!("adder");
}

#[derive(Default, Clone, Debug)]
pub struct AdderState {
    n: u32,
}

#[repc::async_trait]
impl pb::adder_server::Adder for AdderState {
    async fn add(
        &mut self,
        req: pb::AddRequest,
    ) -> Result<tonic::Response<pb::AddResponse>, tonic::Status> {
        self.n += req.i;
        Ok(tonic::Response::new(pb::AddResponse { n: self.n }))
    }
}
