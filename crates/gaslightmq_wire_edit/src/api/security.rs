//! The APIs of authentication, ACLs, delegation tokens and SCRAM credentials.
//! They have no parts, and only fail as a whole.

use gaslightmq_wire::messages::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;
use gaslightmq_wire::messages::create_acls_response::AclCreationResult;
use gaslightmq_wire::messages::delete_acls_response::DeleteAclsFilterResult;
use gaslightmq_wire::messages::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, CreateAclsRequest, CreateAclsResponse,
    CreateDelegationTokenRequest, DeleteAclsRequest, DeleteAclsResponse, DescribeAclsRequest,
    DescribeDelegationTokenRequest, DescribeUserScramCredentialsRequest, ExpireDelegationTokenRequest,
    RenewDelegationTokenRequest, SaslAuthenticateRequest, SaslHandshakeRequest,
};

use super::fails_as_a_whole;
use crate::body::{Editable, Verdict};

fails_as_a_whole!(
    CreateDelegationTokenRequest,
    DescribeAclsRequest,
    DescribeDelegationTokenRequest,
    DescribeUserScramCredentialsRequest,
    ExpireDelegationTokenRequest,
    RenewDelegationTokenRequest,
    SaslAuthenticateRequest,
    SaslHandshakeRequest,
);

impl Editable for CreateAclsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> CreateAclsResponse {
        let results = self
            .creations
            .iter()
            .map(|_| AclCreationResult::default().with_error_code(verdict.call()));
        CreateAclsResponse::default().with_results(results.collect())
    }
}

impl Editable for DeleteAclsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> DeleteAclsResponse {
        let results = self
            .filters
            .iter()
            .map(|_| DeleteAclsFilterResult::default().with_error_code(verdict.call()));
        DeleteAclsResponse::default().with_filter_results(results.collect())
    }
}

impl Editable for AlterUserScramCredentialsRequest {
    fn failed(&self, _version: i16, verdict: &Verdict<'_>) -> AlterUserScramCredentialsResponse {
        let deleted = self.deletions.iter().map(|deletion| &deletion.name);
        let upserted = self.upsertions.iter().map(|upsertion| &upsertion.name);
        let results = deleted.chain(upserted).map(|user| {
            AlterUserScramCredentialsResult::default()
                .with_user(user.clone())
                .with_error_code(verdict.call())
        });
        AlterUserScramCredentialsResponse::default().with_results(results.collect())
    }
}
