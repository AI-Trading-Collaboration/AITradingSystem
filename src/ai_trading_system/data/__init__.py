"""Data ingestion and storage adapters."""

from ai_trading_system.data.download_publication import (
    DownloadArtifactCandidate,
    DownloadLegacyProjectionError,
    DownloadPublicationError,
    DownloadPublicationIntegrityError,
    DownloadReplayInputCandidate,
    DownloadSourceBinding,
    ValidatedDownloadPublication,
    publish_download_transaction,
    resolve_download_publication,
    resolve_download_publication_if_present,
)
from ai_trading_system.data.quality_consumer_authorization import (
    DAILY_SCORE_CONSUMER_AUTHORIZATION_TOKEN,
    DAILY_SCORE_CONSUMER_ID,
    DAILY_SCORE_CONSUMER_VERSION,
    DataQualityConsumerAuthorizationError,
    build_data_quality_consumer_authorization_attestation,
    load_reviewed_data_quality_consumer_authorization_policy,
    verify_data_quality_consumer_authorization,
    write_data_quality_consumer_authorization_attestation,
)

__all__ = [
    "DownloadArtifactCandidate",
    "DownloadLegacyProjectionError",
    "DownloadPublicationError",
    "DownloadPublicationIntegrityError",
    "DownloadReplayInputCandidate",
    "DownloadSourceBinding",
    "DAILY_SCORE_CONSUMER_AUTHORIZATION_TOKEN",
    "DAILY_SCORE_CONSUMER_ID",
    "DAILY_SCORE_CONSUMER_VERSION",
    "DataQualityConsumerAuthorizationError",
    "ValidatedDownloadPublication",
    "build_data_quality_consumer_authorization_attestation",
    "load_reviewed_data_quality_consumer_authorization_policy",
    "publish_download_transaction",
    "resolve_download_publication",
    "resolve_download_publication_if_present",
    "verify_data_quality_consumer_authorization",
    "write_data_quality_consumer_authorization_attestation",
]
