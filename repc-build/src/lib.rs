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

pub struct Config {
    out_dir: Option<PathBuf>,
    format: bool,
}

impl Config {
    pub fn new() -> Self {
        Config {
            out_dir: None,
            format: true,
        }
    }

    pub fn out_dir<P>(mut self, path: P) -> Self
    where
        P: Into<PathBuf>,
    {
        self.out_dir = Some(path.into());
        self
    }

    pub fn compile<P>(self, protos: &[P], includes: &[P]) -> io::Result<()>
    where
        P: AsRef<Path>,
    {
        let Config { out_dir, format } = self;
        let out_dir = if let Some(out_dir) = out_dir {
            out_dir
        } else {
            PathBuf::from(std::env::var("OUT_DIR").expect("expected OUT_DIR envvar is defined"))
        };

        let mut prost_config = prost_build::Config::new();

        prost_config.out_dir(out_dir.clone());
        let generator_config = ServiceGeneratorConfig::new();
        prost_config.service_generator(Box::new(ServiceGenerator::new(generator_config)));
        prost_config.compile_protos(protos, includes)?;

        if format {
            tonic_build::fmt(
                out_dir
                    .to_str()
                    .expect("expected outdir is encoded in utf-8"),
            );
        }

        Ok(())
    }
}

pub fn compile_protos<P>(proto: P) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let proto_path = proto.as_ref();
    let proto_dir = proto_path.parent().unwrap();
    Config::new().compile(&[proto_path], &[proto_dir])
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
