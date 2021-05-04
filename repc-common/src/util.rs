use tonic::{Request, Response};

pub fn clone_request<T: Clone>(request: &Request<T>) -> Request<T> {
    let mut clone = Request::new(request.get_ref().clone());
    *clone.metadata_mut() = request.metadata().clone();
    clone
}

pub fn clone_response<T: Clone>(response: &Response<T>) -> Response<T> {
    let mut clone = Response::new(response.get_ref().clone());
    *clone.metadata_mut() = response.metadata().clone();
    clone
}
