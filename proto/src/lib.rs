pub mod name {
    use tonic::include_proto;
    include_proto!("name_node");
}

pub mod data {
    use tonic::include_proto;
    include_proto!("data_node");
}
