/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use allocative::Allocative;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_error::TypedContext;

/// The digests remote execution reported missing from the CAS while buck2 uploaded or
/// downloaded an action's declared inputs or outputs.
///
/// Each digest is recorded as its canonical `hash:size` string. A caller in any crate compares
/// this string against a digest index with ordinary string equality, without linking against the
/// digest-config generics that produced the original `TrackedCasDigest`.
///
/// Buck2 attaches this as typed context on the `buck2_error::Error` returned from the upload and
/// materialization failure sites. A caller further up the stack pulls the missing set back out
/// with [`buck2_error::Error::find_typed_context`] and identifies which actions produced it.
#[derive(Allocative, Debug, Clone, buck2_error::Error)]
#[error("{} artifact(s) missing from the RE CAS", .missing.len())]
#[buck2(tag = ReCasArtifactMissingRecoverable)]
pub struct MissingCasDigests {
    /// Each entry pairs the project-relative path buck2 tried to materialize or upload with the
    /// digest remote execution reported missing for it.
    pub missing: Vec<(ProjectRelativePathBuf, String)>,
}

impl TypedContext for MissingCasDigests {
    fn eq(&self, other: &dyn TypedContext) -> bool {
        match (other as &dyn std::any::Any).downcast_ref::<Self>() {
            Some(right) => self.missing == right.missing,
            None => false,
        }
    }

    fn display(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use buck2_error::buck2_error;

    use super::*;

    fn missing_entry(path: &str, digest: &str) -> (ProjectRelativePathBuf, String) {
        (
            ProjectRelativePathBuf::unchecked_new(path.to_owned()),
            digest.to_owned(),
        )
    }

    #[test]
    fn survives_as_typed_context_through_wrapping() {
        let missing = vec![missing_entry("buck-out/foo", "aa:1")];
        let context = MissingCasDigests {
            missing: missing.clone(),
        };

        let error: buck2_error::Error = buck2_error!(
            buck2_error::ErrorTag::ReCasArtifactMissingRecoverable,
            "artifact missing"
        )
        .context(context);

        // A caller several layers up wraps the error again, exactly like `ExecuteError` and
        // `ActionError` do on the way from the failure site to the user-visible error.
        let wrapped = error.context("uploading action inputs");

        let recovered = wrapped
            .find_typed_context::<MissingCasDigests>()
            .expect("typed context should survive wrapping");
        assert_eq!(recovered.missing, missing);
    }

    #[test]
    fn absent_when_never_attached() {
        let error: buck2_error::Error = buck2_error!(
            buck2_error::ErrorTag::ReCasArtifactMissingRecoverable,
            "unrelated failure"
        )
        .into();

        assert!(error.find_typed_context::<MissingCasDigests>().is_none());
    }

    #[test]
    fn survives_the_real_materialization_error_chain() {
        use std::sync::Arc;

        use buck2_common::cas_digest::CasDigest;
        use buck2_common::cas_digest::CasDigestConfig;
        use buck2_common::file_ops::metadata::FileMetadata;
        use buck2_common::file_ops::metadata::TrackedFileDigest;
        use buck2_core::execution_types::executor_config::RemoteExecutorUseCase;
        use buck2_directory::directory::entry::DirectoryEntry;

        use crate::directory::ActionDirectoryMember;
        use crate::materialize::materializer::CasDownloadInfo;
        use crate::materialize::materializer::CasMissingRecoveryGuidance;
        use crate::materialize::materializer::CasNotFoundError;
        use crate::materialize::materializer::MaterializationError;

        let missing = vec![missing_entry("buck-out/foo", "aa:1")];
        let context = MissingCasDigests {
            missing: missing.clone(),
        };

        // Mirrors `DefaultIoHandler::materialize_entry`: attach the typed context onto the RE
        // NOT_FOUND error before wrapping it into `CasNotFoundError`.
        let inner: buck2_error::Error = buck2_error!(
            buck2_error::ErrorTag::ReCasArtifactMissingRecoverable,
            "artifact not found"
        )
        .context(context);

        let digest = TrackedFileDigest::new(
            CasDigest::new_sha1([0; 20], 1),
            CasDigestConfig::testing_default(),
        );
        let source = CasNotFoundError {
            path: Arc::new(ProjectRelativePathBuf::unchecked_new(
                "buck-out/foo".to_owned(),
            )),
            info: Arc::new(CasDownloadInfo::new_declared(
                RemoteExecutorUseCase::buck2_default(),
            )),
            directory: DirectoryEntry::Leaf(ActionDirectoryMember::File(FileMetadata {
                digest,
                is_executable: false,
            })),
            recovery: CasMissingRecoveryGuidance::RestartRequired,
            error: Arc::new(inner),
        };

        // Mirrors the executor layer: `MaterializationError::NotFound` converts to
        // `buck2_error::Error` through the same `Arc<Error>` clone that production code goes
        // through when a materialization failure becomes an `ExecuteError`.
        let error: buck2_error::Error = MaterializationError::NotFound { source }.into();

        let recovered = error
            .find_typed_context::<MissingCasDigests>()
            .expect("typed context should survive the real materialization error chain");
        assert_eq!(recovered.missing, missing);
    }

    #[test]
    fn does_not_appear_in_formatted_message() {
        let context = MissingCasDigests {
            missing: vec![missing_entry("buck-out/foo", "aa:1")],
        };
        let error: buck2_error::Error = buck2_error!(
            buck2_error::ErrorTag::ReCasArtifactMissingRecoverable,
            "upload failed"
        )
        .context(context);

        // The typed context holds structured data for programmatic use; it must not leak into
        // the human-readable error text a second time.
        assert!(format!("{error:#}").matches("artifact(s) missing").count() == 0);
    }
}
