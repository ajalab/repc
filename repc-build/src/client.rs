use proc_macro2::TokenStream;
use prost_build::Service;
use quote::quote;

pub fn generate(service: &Service, proto_path: &str) -> TokenStream {
    quote! {}
}
