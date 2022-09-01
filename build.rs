fn main() {
    // when build.rs support check cfg test, enable it
    /*tonic_build::configure()
    .build_client(true)
    .build_server(false)
    .compile(&["proto/agent.proto"], &["proto/"])
    .unwrap();*/
    tonic_build::compile_protos("proto/agent.proto").unwrap();
}
