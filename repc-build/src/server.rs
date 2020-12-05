use super::camel_to_snake;
use proc_macro2::{Ident, Span, TokenStream};
use prost_build::Service;
use quote::{quote, ToTokens};

pub fn generate(service: &Service, proto_path: &str) -> TokenStream {
    let service_name = if service.package.is_empty() {
        service.proto_name.clone()
    } else {
        format!("{}.{}", service.package, service.proto_name)
    };
    let generated_trait = generate_trait(service, proto_path);
    let server_mod = quote::format_ident!("{}_server", camel_to_snake(&service.name));

    quote! {
        pub mod #server_mod {
            #generated_trait
        }
    }
}

fn generate_trait(service: &Service, proto_path: &str) -> TokenStream {
    let name = Ident::new(&service.name, Span::call_site());
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
        let name = &method.name;
        let req_message = resolve_message(proto_path, &method.input_proto_type);
        let res_message = resolve_message(proto_path, &method.output_proto_type);
        let method = match (method.client_streaming, method.server_streaming) {
            (false, false) => {
                quote! {
                    fn #name(&mut self, request: #req_message) -> Result<tonic::Response<#res_message>, tonic::Status>;
                }
            }
            _ => {
                unimplemented!();
            }
        };
        stream.extend(method);
    }
    stream
}

fn resolve_message(proto_path: &str, proto_type: &str) -> TokenStream {
    if proto_type.starts_with(".google.protobuf") || proto_type.starts_with("::") {
        proto_type.parse::<TokenStream>().unwrap()
    } else {
        syn::parse_str::<syn::Path>(&format!("{}::{}", proto_path, proto_type))
            .unwrap()
            .to_token_stream()
    }
}
