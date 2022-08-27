fn main() {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile(&["../proto/agent.proto"], &["../proto/"])
        .unwrap();
}
