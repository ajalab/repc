use super::camel_to_snake;
use proc_macro2::{Ident, Span, TokenStream};
use prost_build::Service;
use quote::{quote, ToTokens};

pub fn generate(service: &Service, proto_path: &str) -> TokenStream {
    let trait_def = generate_trait_def(service, proto_path);
    let state_machine_def = generate_state_machine_def(service);
    let state_machine_impl = generate_state_machine_impl(service);
    let state_machine_trait_impl = generate_state_machine_trait_impl(service);
    let server_mod = quote::format_ident!("{}_server", camel_to_snake(&service.name));

    quote! {
        pub mod #server_mod {
            #trait_def

            #state_machine_def

            #state_machine_impl

            #state_machine_trait_impl
        }
    }
}

fn generate_trait_def(service: &Service, proto_path: &str) -> TokenStream {
    let name = resolve_trait_name(service);
    let methods = generate_trait_methods(service, proto_path);

    quote! {
        pub trait #name {
            #methods
        }
    }
}

fn generate_trait_methods(service: &Service, proto_path: &str) -> TokenStream {
    let mut stream = TokenStream::new();
    for method in &service.methods {
        let name = Ident::new(&method.name, Span::call_site());
        let req_message = resolve_message(proto_path, &method.input_proto_type, &method.input_type);
        let res_message =
            resolve_message(proto_path, &method.output_proto_type, &method.output_type);
        let method = match (method.client_streaming, method.server_streaming) {
            (false, false) => {
                quote! {
                    fn #name(&mut self, request: #req_message) -> Result<tonic::Response<#res_message>, tonic::Status>;
                }
            }
            _ => {
                unimplemented!("streaming is not supported yet");
            }
        };
        stream.extend(method);
    }
    stream
}

fn generate_state_machine_def(service: &Service) -> TokenStream {
    let name = resolve_state_machine_name(service);

    quote! {
        pub struct #name<T> {
            inner: T
        }
    }
}

fn generate_state_machine_impl(service: &Service) -> TokenStream {
    let name = resolve_state_machine_name(service);

    quote! {
        impl<T> #name<T> {
            pub fn new(inner: T) -> Self {
                Self { inner }
            }
        }
    }
}

fn generate_state_machine_trait_impl(service: &Service) -> TokenStream {
    let trait_name = resolve_trait_name(service);
    let state_machine_name = resolve_state_machine_name(service);
    let match_arms = genearte_state_machine_impl_match_arms(service);

    quote! {
        impl<T> repc::codegen::StateMachine for #state_machine_name<T>
        where
            T: #trait_name,
        {
            fn apply(
                &mut self,
                path: &str,
                body: &[u8],
            ) -> Result<tonic::Response<bytes::Bytes>, repc::codegen::StateMachineError> {
                match path {
                    #match_arms
                }
            }
        }
    }
}

fn genearte_state_machine_impl_match_arms(service: &Service) -> TokenStream {
    let path_parent = if service.package.is_empty() {
        format!("/{}", service.proto_name)
    } else {
        format!("/{}.{}", service.package, service.proto_name)
    };
    let mut stream = TokenStream::new();

    for method in &service.methods {
        let path = format!("{}/{}", path_parent, method.proto_name);
        let method_name = Ident::new(&method.name, Span::call_site());
        let arm = quote! {
            #path => repc::codegen::handle_request(body, |req| self.inner.#method_name(req)),
        };
        stream.extend(arm);
    }
    stream.extend(quote! {
        _ => Err(repc::codegen::StateMachineError::UnknownPath(path.into()))
    });
    stream
}

fn resolve_trait_name(service: &Service) -> Ident {
    Ident::new(&service.name, Span::call_site())
}

fn resolve_state_machine_name(service: &Service) -> Ident {
    quote::format_ident!("{}StateMachine", service.name)
}

fn resolve_message(proto_path: &str, proto_type: &str, ty: &str) -> TokenStream {
    if proto_type.starts_with(".google.protobuf") || proto_type.starts_with("::") {
        proto_type.parse::<TokenStream>().unwrap()
    } else {
        syn::parse_str::<syn::Path>(&format!("{}::{}", proto_path, ty))
            .unwrap()
            .to_token_stream()
    }
}
