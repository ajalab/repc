use super::util;
use proc_macro2::{Ident, Span, TokenStream};
use prost_build::Service;
use quote::quote;

pub fn generate(service: &Service, proto_path: &str) -> TokenStream {
    let mod_name = quote::format_ident!("{}_client", util::camel_to_snake(&service.name));
    let type_defs = generate_type_defs();
    let client_def = generate_client_def(service);
    let client_impl = generate_client_impl(service);
    let client_service_impl = generate_client_service_impl(service, proto_path);
    let client_channel_impl = generate_client_channel_impl(service);
    quote! {
        pub mod #mod_name {
            #type_defs

            #client_def

            #client_impl

            #client_service_impl

            #client_channel_impl
        }
    }
}

fn generate_type_defs() -> TokenStream {
    quote! {
        type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
        use repc_client::{
            RepcClient,
            configuration::Configuration,
            codegen::{TonicBody, HttpBody, NodeId},
            error::ToChannelError,
        };
    }
}

fn generate_client_def(service: &Service) -> TokenStream {
    let name = resolve_client_name(service);
    quote! {
        pub struct #name<T> {
            inner: RepcClient<T>,
        }
    }
}

fn generate_client_impl(service: &Service) -> TokenStream {
    // TODO: Remove this. This is for testing use - get the inner client to perform client registration.
    let name = resolve_client_name(service);
    quote! {
        impl<T> #name<T>
        {
            pub fn get_mut(&mut self) -> &mut RepcClient<T> {
                &mut self.inner
            }
        }
    }
}

fn generate_client_service_impl(service: &Service, proto_path: &str) -> TokenStream {
    let name = resolve_client_name(service);
    let client_method_new = generate_client_method_new();
    let client_methods_proto = generate_client_methods_proto(service, proto_path);
    quote! {
        impl<T> #name<T>
        where
            T: tonic::client::GrpcService<tonic::body::BoxBody>,
            T::ResponseBody: TonicBody + HttpBody + Send + 'static,
            T::Error: Into<StdError>,
            <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
        {
            #client_method_new

            #client_methods_proto
        }
    }
}

fn generate_client_method_new() -> TokenStream {
    quote! {
        pub fn new<I>(services: I) -> Self
        where
            I: IntoIterator<Item = (NodeId, T)>,
        {
            Self {
                inner: RepcClient::new(services),
            }
        }
    }
}

fn generate_client_methods_proto(service: &Service, proto_path: &str) -> TokenStream {
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

fn generate_client_channel_impl(service: &Service) -> TokenStream {
    let name = resolve_client_name(service);
    quote! {
        impl #name<tonic::transport::Channel>
        {
            pub fn from_conf(conf: Configuration) -> Result<Self, ToChannelError> {
                let inner = RepcClient::from_conf(conf)?;
                Ok(Self { inner })
            }
        }
    }
}

fn resolve_client_name(service: &Service) -> Ident {
    quote::format_ident!("{}Client", service.name)
}
