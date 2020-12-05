pub mod client;
pub mod server;

use proc_macro2::TokenStream;
use prost_build::Service;
use quote::quote;
use std::io;
use std::path::{Path, PathBuf};

struct ServiceGenerator {
    config: ServiceGeneratorConfig,
    clients: TokenStream,
    servers: TokenStream,
}

impl ServiceGenerator {
    fn new(config: ServiceGeneratorConfig) -> Self {
        ServiceGenerator {
            config,
            clients: TokenStream::default(),
            servers: TokenStream::default(),
        }
    }
}

struct ServiceGeneratorConfig {
    proto_path: String,
    build_server: bool,
    build_client: bool,
}

impl ServiceGeneratorConfig {
    fn new() -> Self {
        ServiceGeneratorConfig {
            proto_path: "super".to_owned(),
            build_server: true,
            build_client: true,
        }
    }
}

impl prost_build::ServiceGenerator for ServiceGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        if self.config.build_server {
            let code = server::generate(&service, &self.config.proto_path);
            let code = (quote! { #code }).to_string();
            buf.push_str(&code);
        }
        if self.config.build_client {
            let code = client::generate(&service, &self.config.proto_path);
            let code = (quote! { #code }).to_string();
            buf.push_str(&code);
        }
    }
}

pub fn configure() -> Config {
    Config {
        prost_config: prost_build::Config::new(),
        generator_config: ServiceGeneratorConfig::new(),
    }
}

pub fn compile_protos<P>(proto: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let proto_path = proto.as_ref();
    let proto_dir = proto_path.parent().unwrap();
    configure().compile(&[proto_path], &[proto_dir])
}

pub struct Config {
    prost_config: prost_build::Config,
    generator_config: ServiceGeneratorConfig,
}

impl Config {
    pub fn compile<P>(self, protos: &[P], includes: &[P]) -> io::Result<()>
    where
        P: AsRef<Path>,
    {
        let Config {
            prost_config: mut config,
            generator_config,
        } = self;
        config.service_generator(Box::new(ServiceGenerator::new(generator_config)));
        config.compile_protos(protos, includes)
    }

    pub fn out_dir<P>(&mut self, path: P) -> &mut Self
    where
        P: Into<PathBuf>,
    {
        self.prost_config.out_dir(path);
        self
    }
}

fn camel_to_snake(s: &str) -> String {
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
