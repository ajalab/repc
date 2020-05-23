use std::sync::Arc;

#[derive(Default, Debug)]
struct Configuration {
    a: u32,
}

fn f(conf: Arc<Configuration>) {
    tokio::spawn(async move {
        println!("conf: {:?}", conf);
    });
}

#[tokio::main]
async fn main() {
    let conf1 = Arc::new(Configuration::default());

    f(conf1.clone());
    f(conf1.clone());
}