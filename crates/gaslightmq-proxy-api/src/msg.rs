//! WebSocket messages exchanged on `GET /api/ws`, encoded as JSON.

use serde::{Deserialize, Serialize};

use crate::model::{ApiRow, NodeInfo, NodeState, Snapshot, StatsTick};

/// Proxy → UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    /// Full state, sent once right after the socket opens.
    Snapshot(Snapshot),
    /// A row changed (from any UI, the REST API or code).
    RowChanged(ApiRow),
    /// All rows were cleared.
    RowsCleared,
    /// The node list or a node's state changed.
    Nodes { nodes: Vec<NodeInfo> },
    /// Periodic activity counters.
    Stats(StatsTick),
}

/// UI → proxy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    SetRow(ApiRow),
    SetNodeState { node_id: i32, state: NodeState },
    ClearAll,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ErrorPad, StatsTick};

    #[test]
    fn client_msgs_round_trip_as_tagged_json() {
        let mut row = ApiRow::new(0);
        row.error = ErrorPad { enabled: true, probability: 50, code: 6 };
        let msgs = [
            ClientMsg::SetRow(row),
            ClientMsg::SetNodeState { node_id: 2, state: NodeState::Down },
            ClientMsg::ClearAll,
        ];
        for msg in msgs {
            let json = serde_json::to_string(&msg).unwrap();
            assert!(json.contains("\"type\""), "{json}");
            assert_eq!(serde_json::from_str::<ClientMsg>(&json).unwrap(), msg);
        }
    }

    #[test]
    fn server_msgs_round_trip() {
        let msgs = [
            ServerMsg::Nodes { nodes: vec![] },
            ServerMsg::RowsCleared,
            ServerMsg::Stats(StatsTick { seq: 3, ..Default::default() }),
        ];
        for msg in msgs {
            let json = serde_json::to_string(&msg).unwrap();
            assert_eq!(serde_json::from_str::<ServerMsg>(&json).unwrap(), msg);
        }
        let json = serde_json::to_string(&ServerMsg::RowsCleared).unwrap();
        assert_eq!(json, r#"{"type":"rows_cleared"}"#);
    }
}
