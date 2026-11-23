//! What every client asks a broker first.

use gaslightmq_backend::prelude::*;
use gaslightmq_backend_upstream::prelude::*;
use gaslightmq_environment::prelude::*;
use gaslightmq_field::prelude::*;
use gaslightmq_test_kafka::KafkaCluster;
use gaslightmq_wire::messages::{ApiVersionsRequest, ApiVersionsResponse};
use gaslightmq_wire::protocol::StrBytes;
use gaslightmq_wire::{ApiKey, Request};

#[tokio::test]
async fn a_client_learns_the_versions_of_the_real_broker() -> anyhow::Result<()> {
    let cluster = KafkaCluster::plaintext().start().await?;
    let upstream = Upstream::new(cluster.brokers()[0].address().to_string().parse()?);
    let configuration = UpstreamBackendConfiguration::new(upstream, "gaslight.test".parse()?);
    let backend = UpstreamBackend::start(configuration).await?;

    let greeting = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("order-ingest"))
        .with_client_software_version(StrBytes::from_static_str("1.0.0"));
    let request = Request::new(3, 1, Some("order-ingest"), &greeting)?;
    let answer = backend
        .call(BackendRequest::new("#1.1".parse()?, Endpoint::Bootstrap, request))
        .await;

    let BackendAnswer::Truth(truth) = answer else {
        anyhow::bail!("the cluster answers an ApiVersions request, and the backend made of it: {answer:?}");
    };
    let versions: ApiVersionsResponse = truth.response().body()?;
    assert_eq!(versions.error_code, 0);
    for api in [ApiKey::Produce, ApiKey::Fetch, ApiKey::Metadata] {
        assert!(
            versions.api_keys.iter().any(|known| known.api_key == api as i16),
            "the broker knows {api:?}"
        );
    }
    assert!(
        truth.upstream().is_none(),
        "the answer tells no address, so the overlay leaves it as it is"
    );
    Ok(())
}
