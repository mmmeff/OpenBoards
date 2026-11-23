use std::fmt;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    InternalError,
    // Board errors
    NoBoard,
    BoardNotFound,
    BoardNameTaken,
    BoardNotGranted,
    // Channel & grant errors
    ChannelExists,
    ChannelGranted,
    GrantNotFound,
    // Message / thread / label / schema / task / sync errors (reserved for Waves 2+)
    MessageNotFound,
    PostNotFound,
    MessageDeleted,
    NewerMessage,
    RevisionConflict,
    LabelConflict,
    ReservedLabel,
    DuplicateId,
    InvalidBlock,
    SchemaNotFound,
    SchemaImmutable,
    ClaimLost,
    TaskNotOwned,
    InvalidNeeds,
    BatchTooLarge,
    AlreadySynced,
    WatchersNeedProject,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::InternalError => "internal_error",
            Self::NoBoard => "no_board",
            Self::BoardNotFound => "board_not_found",
            Self::BoardNameTaken => "board_name_taken",
            Self::BoardNotGranted => "board_not_granted",
            Self::ChannelExists => "channel_exists",
            Self::ChannelGranted => "channel_granted",
            Self::GrantNotFound => "grant_not_found",
            Self::MessageNotFound => "message_not_found",
            Self::PostNotFound => "post_not_found",
            Self::MessageDeleted => "message_deleted",
            Self::NewerMessage => "newer_message",
            Self::RevisionConflict => "revision_conflict",
            Self::LabelConflict => "label_conflict",
            Self::ReservedLabel => "reserved_label",
            Self::DuplicateId => "duplicate_id",
            Self::InvalidBlock => "invalid_block",
            Self::SchemaNotFound => "schema_not_found",
            Self::SchemaImmutable => "schema_immutable",
            Self::ClaimLost => "claim_lost",
            Self::TaskNotOwned => "task_not_owned",
            Self::InvalidNeeds => "invalid_needs",
            Self::BatchTooLarge => "batch_too_large",
            Self::AlreadySynced => "already_synced",
            Self::WatchersNeedProject => "watchers_need_project",
        }
    }

    pub fn default_status(&self) -> StatusCode {
        match self {
            Self::BadRequest
            | Self::MessageDeleted
            | Self::ReservedLabel
            | Self::InvalidBlock
            | Self::SchemaNotFound
            | Self::InvalidNeeds
            | Self::BatchTooLarge => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden
            | Self::BoardNotGranted
            | Self::TaskNotOwned
            | Self::WatchersNeedProject => StatusCode::FORBIDDEN,
            Self::NotFound
            | Self::NoBoard
            | Self::BoardNotFound
            | Self::GrantNotFound
            | Self::MessageNotFound
            | Self::PostNotFound => StatusCode::NOT_FOUND,
            Self::Conflict
            | Self::BoardNameTaken
            | Self::ChannelExists
            | Self::ChannelGranted
            | Self::NewerMessage
            | Self::RevisionConflict
            | Self::LabelConflict
            | Self::DuplicateId
            | Self::SchemaImmutable
            | Self::ClaimLost
            | Self::AlreadySynced => StatusCode::CONFLICT,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoardError {
    pub code: ErrorCode,
    pub status: StatusCode,
    pub message: String,
    pub hint: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl BoardError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status: code.default_status(),
            code,
            message: message.into(),
            hint: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthorized, message)
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalError, message)
    }

    pub fn body(&self) -> ErrorBody {
        ErrorBody {
            code: self.code.as_str().to_string(),
            message: self.message.clone(),
            hint: self.hint.clone(),
        }
    }
}

impl fmt::Display for BoardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for BoardError {}

impl From<rusqlite::Error> for BoardError {
    fn from(err: rusqlite::Error) -> Self {
        Self::internal(format!("database error: {err}"))
    }
}

impl IntoResponse for BoardError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body())).into_response()
    }
}

pub type BoardResult<T> = Result<T, BoardError>;

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn error_codes_map_to_expected_http_statuses() {
        assert_eq!(ErrorCode::BadRequest.default_status(), StatusCode::BAD_REQUEST);
        assert_eq!(ErrorCode::Unauthorized.default_status(), StatusCode::UNAUTHORIZED);
        assert_eq!(ErrorCode::Forbidden.default_status(), StatusCode::FORBIDDEN);
        assert_eq!(ErrorCode::BoardNotFound.default_status(), StatusCode::NOT_FOUND);
        assert_eq!(ErrorCode::NoBoard.default_status(), StatusCode::NOT_FOUND);
        assert_eq!(ErrorCode::BoardNameTaken.default_status(), StatusCode::CONFLICT);
        assert_eq!(ErrorCode::ChannelExists.default_status(), StatusCode::CONFLICT);
        assert_eq!(ErrorCode::ChannelGranted.default_status(), StatusCode::CONFLICT);
        assert_eq!(ErrorCode::ReservedLabel.default_status(), StatusCode::BAD_REQUEST);
        assert_eq!(ErrorCode::LabelConflict.default_status(), StatusCode::CONFLICT);
        assert_eq!(ErrorCode::BoardNotGranted.default_status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn board_error_serializes_json_contract() {
        let err = BoardError::new(ErrorCode::BoardNotFound, "board 'security' not found")
            .with_hint("Pass ?ensure=true to create the board.");
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "board_not_found");
        assert_eq!(body["message"], "board 'security' not found");
        assert_eq!(body["hint"], "Pass ?ensure=true to create the board.");
    }

    #[tokio::test]
    async fn board_error_omits_absent_hint() {
        let err = BoardError::unauthorized("missing bearer token");
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["code"], "unauthorized");
        assert!(body.get("hint").is_none());
    }
}
