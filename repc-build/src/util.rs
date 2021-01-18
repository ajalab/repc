use proc_macro2::TokenStream;
use prost_build::{Method, Service};
use quote::ToTokens;

pub fn resolve_message(proto_path: &str, proto_type: &str, ty: &str) -> TokenStream {
    if proto_type.starts_with(".google.protobuf") || proto_type.starts_with("::") {
        proto_type.parse::<TokenStream>().unwrap()
    } else {
        syn::parse_str::<syn::Path>(&format!("{}::{}", proto_path, ty))
            .unwrap()
            .to_token_stream()
    }
}

pub fn resolve_method_path(service: &Service, method: &Method) -> String {
    if service.package.is_empty() {
        format!("/{}/{}", service.proto_name, method.proto_name)
    } else {
        format!(
            "/{}.{}/{}",
            service.package, service.proto_name, method.proto_name
        )
    }
}

pub fn camel_to_snake(s: &str) -> String {
    let mut buf = String::new();
    let mut iter = s.chars();

    if let Some(c) = iter.next() {
        buf.push(c.to_ascii_lowercase());
    }
    while let Some(c) = iter.next() {
        let l = c.to_ascii_lowercase();
        if c != l {
            buf.push('_');
        }
        buf.push(l);
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::camel_to_snake;
    #[test]
    fn test_camel_to_snake() {
        let cases = &[
            ("", ""),
            ("A", "a"),
            ("Test", "test"),
            ("TestTest", "test_test"),
            ("TestATest", "test_a_test"),
            ("TesTT", "tes_t_t"),
            ("TTest", "t_test"),
        ];

        for &(input, expected) in cases {
            assert_eq!(camel_to_snake(input), expected);
        }
    }
}
