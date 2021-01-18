use super::util;
use proc_macro2::{Ident, Span, TokenStream};
use prost_build::Service;
use quote::quote;

pub fn generate(service: &Service, proto_path: &str) -> TokenStream {
    let mod_name = quote::format_ident!("{}_client", util::camel_to_snake(&service.name));
    let type_defs = generate_type_defs();
    let client_def = generate_client_def(service);
    let client_impl = generate_client_impl(service, proto_path);
    quote! {
        pub mod #mod_name {
            #type_defs

            #client_def

            #client_impl
        }
    }
}

fn generate_type_defs() -> TokenStream {
    quote! {
        type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
        use repc_client::{
            RepcClient,
            error::RegisterError,
            codegen::{TonicBody, HttpBody}
        };
    }
}

fn generate_client_def(service: &Service) -> TokenStream {
    let name = resolve_client_name(service);
    quote! {
        pub struct #name<T> {
            inner: repc_client::RepcClient<T>,
        }
    }
}

fn generate_client_impl(service: &Service, proto_path: &str) -> TokenStream {
    let name = resolve_client_name(service);
    let client_register_method = generate_client_register_method();
    let client_proto_methods = generate_client_proto_methods(service, proto_path);
    quote! {
        impl<T> #name<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: TonicBody + HttpBody + Send + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
        {
            #client_register_method

            #client_proto_methods
        }
    }
}

fn generate_client_register_method() -> TokenStream {
    quote! {
        pub async fn register(service: T) -> Result<Self, RegisterError> {
            RepcClient::register(service)
                .await
                .map(|client| {
                    Self { inner: client }
            })
        }
    }
}

fn generate_client_proto_methods(service: &Service, proto_path: &str) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in &service.methods {
        let name = Ident::new(&method.name, Span::call_site());
        let req_message =
            util::resolve_message(proto_path, &method.input_proto_type, &method.input_type);
        let res_message =
            util::resolve_message(proto_path, &method.output_proto_type, &method.output_type);
        let path = util::resolve_method_path(service, method);
        let method = match (method.client_streaming, method.server_streaming) {
            (false, false) => {
                quote! {
                    pub async fn #name(
                        &mut self,
                        request: impl tonic::IntoRequest<#req_message>,
                    ) -> Result<tonic::Response<#res_message>, tonic::Status> {
                        self.inner.unary(#path, request).await
                    }
                }
            }
            _ => unimplemented!("streaming is not supported yet"),
        };
        stream.extend(method)
    }

    stream
}

fn resolve_client_name(service: &Service) -> Ident {
    quote::format_ident!("{}Client", service.name)
}
