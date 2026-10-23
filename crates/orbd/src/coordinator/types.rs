//! Coordinator-internal records: requests, stored results, command rows.

use crate::model::{
    Action, CommandOutcome, FailureCode, InvocationId, OperationId, OrbId, OrbState,
    ProcessDisposition, SnapshotDisposition, StableOrbState,
};
use orbd_proto::control::v1 as pb;
use serde::{Deserialize, Serialize};

pub const DISPATCH_BUDGET_MS: u64 = crate::model::DISPATCH_BUDGET.as_millis() as u64;
pub const WORK_BUDGET_MS: u64 = crate::model::WORK_BUDGET.as_millis() as u64;
pub const CLEANUP_BUDGET_MS: u64 = crate::model::CLEANUP_BUDGET.as_millis() as u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ReqAction {
    Spawn {
        image_ref: String,
        snapshot_id: Option<String>,
    },
    Pause {
        snapshot_id: Option<String>,
    },
    Delete,
}

impl ReqAction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Spawn { .. } => "spawn",
            Self::Pause { .. } => "pause",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReqStatus {
    Queued,
    Active,
    Finished,
}

impl ReqStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Active => "active",
            Self::Finished => "finished",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "queued" => Self::Queued,
            "active" => Self::Active,
            "finished" => Self::Finished,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Req {
    pub invocation_id: InvocationId,
    pub offering_id: crate::model::OfferingId,
    pub amp_endpoint: crate::model::AmpEndpointId,
    pub amp_orb_id: crate::model::AmpOrbId,
    pub action: ReqAction,
    pub fingerprint: Option<String>,
    pub seq: i64,
    pub start_ms: u64,
    pub work_deadline_ms: u64,
    pub cleanup_deadline_ms: u64,
    pub dispatch_deadline_ms: u64,
    pub status: ReqStatus,
    pub operation_id: Option<OperationId>,
    pub result: Option<StoredResult>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Succeeded,
    Failed,
    Invalid,
    Unavailable,
    OutcomeUnknown,
    DeadlineExceeded,
}

impl Status {
    pub fn label(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Invalid => "invalid",
            Self::Unavailable => "unavailable",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
    pub fn to_pb(self) -> pb::DispatchStatus {
        match self {
            Self::Succeeded => pb::DispatchStatus::Succeeded,
            Self::Failed => pb::DispatchStatus::Failed,
            Self::Invalid => pb::DispatchStatus::Invalid,
            Self::Unavailable => pb::DispatchStatus::Unavailable,
            Self::OutcomeUnknown => pb::DispatchStatus::OutcomeUnknown,
            Self::DeadlineExceeded => pb::DispatchStatus::DeadlineExceeded,
        }
    }
}

/// Final result of a dispatch request, persisted on the request row.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredResult {
    pub status: Status,
    pub operation_id: Option<OperationId>,
    pub outcome: Option<CommandOutcome>,
    pub outcome_known: bool,
    pub message: String,
}

impl StoredResult {
    pub fn to_pb(&self) -> pb::DispatchResult {
        pb::DispatchResult {
            status: self.status.to_pb() as i32,
            operation_id: self.operation_id.map(|o| o.to_string()).unwrap_or_default(),
            outcome: self.outcome.as_ref().map(Into::into),
            outcome_known: self.outcome_known,
            message: self.message.clone(),
        }
    }

    pub fn plain(status: Status, message: impl Into<String>) -> Self {
        Self {
            status,
            operation_id: None,
            outcome: None,
            outcome_known: matches!(
                status,
                Status::Succeeded | Status::Failed | Status::Invalid | Status::Unavailable
            ),
            message: message.into(),
        }
    }

    /// Result for a request whose command reached `outcome`.
    pub fn from_outcome(action: &str, op: OperationId, outcome: &CommandOutcome) -> Self {
        let (status, message) = match &outcome.result {
            Ok(_) => (Status::Succeeded, format!("{action} succeeded")),
            Err(f) => {
                let status = match f.code {
                    FailureCode::BackendUnavailable => Status::Unavailable,
                    FailureCode::InvalidRequest => Status::Invalid,
                    FailureCode::DeadlineExceeded => Status::DeadlineExceeded,
                    _ => Status::Failed,
                };
                (
                    status,
                    format!("{action} failed: {}", failure_text(f.code, &f.message)),
                )
            }
        };
        Self {
            status,
            operation_id: Some(op),
            outcome: Some(outcome.clone()),
            outcome_known: true,
            message,
        }
    }
}

pub fn failure_text(code: FailureCode, message: &str) -> String {
    let msg: String = message
        .chars()
        .filter(|c| !c.is_control())
        .take(1000)
        .collect();
    format!("{code:?}: {msg}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmdStatus {
    Pending,
    Accepted,
    Terminal,
    Unknown,
}

impl CmdStatus {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "pending" => Self::Pending,
            "accepted" => Self::Accepted,
            "terminal" => Self::Terminal,
            "unknown" => Self::Unknown,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CmdRow {
    pub operation_id: OperationId,
    pub orb_id: OrbId,
    pub generation: u64,
    pub action: Action,
    pub command_digest: String,
    pub fingerprint: Option<String>,
    pub status: CmdStatus,
    pub outcome: Option<CommandOutcome>,
    pub submitted_ms: Option<u64>,
    pub work_deadline_ms: u64,
    pub cleanup_deadline_ms: u64,
    pub cancel_requested_ms: Option<u64>,
}

/// Outcome synthesised locally (nothing was executed, or the executor rejected it).
pub fn synthetic_outcome(
    observed: OrbState,
    failure: crate::model::CommandFailure,
) -> CommandOutcome {
    CommandOutcome {
        result: Err(failure),
        observed,
        processes: ProcessDisposition::NotApplicable,
        snapshot: SnapshotDisposition::NotRequested,
    }
}

pub fn is_known(state: &OrbState, want: &StableOrbState) -> bool {
    matches!(state, OrbState::Known { state } if state == want)
}
