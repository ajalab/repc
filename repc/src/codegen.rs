use crate::state_machine::error::StateMachineError;
use std::future::Future;
/// Handles a byte-array request body with a given handler.
pub async fn handle_request<Req, Res, F, H>(
    request_body: &[u8],
    handler: H,
) -> Result<tonic::Response<bytes::Bytes>, StateMachineError>
where
    Req: Default + prost::Message,
    Res: prost::Message,
    F: Future<Output = Result<tonic::Response<Res>, tonic::Status>>,
    H: FnOnce(Req) -> F,
{
    let req = Req::decode(request_body).map_err(|e| StateMachineError::DecodeRequestFailed(e))?;
    let mut res = handler(req).await.map_err(StateMachineError::ApplyFailed)?;
    let mut res_bytes = tonic::Response::new(bytes::BytesMut::new());
    std::mem::swap(res.metadata_mut(), res_bytes.metadata_mut());
    res.into_inner()
        .encode(res_bytes.get_mut())
        .map_err(StateMachineError::EncodeResponseFailed)?;
    Ok(res_bytes.map(bytes::Bytes::from))
}
