//! What the client does against a control plane: it sends an operation and reads the answer,
//! and it follows the live stream.
//!
//! The control plane here is a [`ScriptedControlPlane`] on a free port, which answers what a test tells it to,
//! in the form of the control plane. The tests of `control_server` run the client against the real server.

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use gaslightmq_control_api::clock::TimeSpan;
use gaslightmq_control_api::prelude::*;
use gaslightmq_control_api::selector::{Conditions, SelectorRef};
use gaslightmq_control_api::surface::PresenceId;
use gaslightmq_control_client::{
    ControlClient, ControlClientConfiguration, ControlPlaneAddress, SendError, StreamError,
};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

/// The run of the scripted control plane, as its snapshot writes it.
const RUN: &str = "4be0643f1d98573b97cdca98a65347dd";

/// What the scripted control plane answers a change with: the revision that the change left.
const LEFT: Revised = Revised {
    revision: Revision::new(8),
};

/// The presence that the control plane of a test names for a tab of the UI, and the one it names for a client
/// whose presence it no longer holds.
const TAB: PresenceId = PresenceId::from_random_bits(0x9f86_d081_884c_7d65_9a2f_eaa0_c55a_d015);
const NEW_TAB: PresenceId = PresenceId::from_random_bits(0x2c26_b46b_68ff_c68f_f99b_453c_1d30_4134);

mod operations {
    use super::*;

    #[tokio::test]
    async fn an_operation_that_the_control_plane_refuses_returns_its_own_error() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let arm = ArmRule {
            environment: "orders-sim".parse()?,
            rule: "delay-1s-on-order-producers".parse()?,
            expects: None,
        };

        let (answer, request) = tokio::join!(
            client.send(arm),
            control_plane.refuse(RuleCommandError::NotRunning {
                environment: "orders-sim".parse()?
            }),
        );

        assert_eq!(
            answer?,
            Err(RuleCommandError::NotRunning {
                environment: "orders-sim".parse()?
            })
        );
        let request = request?;
        assert_eq!(request.path, "/api/operations");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request.body)?,
            serde_json::json!({
                "from": { "surface": "api" },
                "operation": "arm-rule",
                "environment": "orders-sim",
                "rule": "delay-1s-on-order-producers",
            }),
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_request_says_the_surface_of_its_client_or_the_source_it_names_itself() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        // The UI is one client: its views send from `ui`, its pads from `pad`, and ⌘Z says that it takes back.
        let mut configuration = ControlClientConfiguration::new(control_plane.address()?);
        configuration.surface = RequestSurface::Ui;
        let ui = ControlClient::new(configuration);
        let arm = ArmRule {
            environment: "orders-sim".parse()?,
            rule: "delay-1s-on-order-producers".parse()?,
            expects: None,
        };
        let disarm = DisarmRule {
            environment: arm.environment.clone(),
            rule: arm.rule.clone(),
            expects: None,
        };
        let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);

        let (_, from_a_view) = tokio::join!(ui.send(arm.clone()), control_plane.answer(LEFT));
        let (_, from_a_pad) = tokio::join!(
            ui.send_from(RequestSurface::Pad, arm.clone()),
            control_plane.answer(LEFT)
        );
        let (_, taken_back) = tokio::join!(ui.send_from(undo, disarm.clone()), control_plane.answer(LEFT));

        assert_eq!(
            serde_json::from_str::<OperationRequest>(&from_a_view?.body)?,
            OperationRequest::new(RequestSurface::Ui, arm.clone()),
        );
        assert_eq!(
            serde_json::from_str::<OperationRequest>(&from_a_pad?.body)?,
            OperationRequest::new(RequestSurface::Pad, arm),
        );
        assert_eq!(
            serde_json::from_str::<OperationRequest>(&taken_back?.body)?,
            OperationRequest::new(undo, disarm),
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_change_says_what_it_expects_and_returns_the_revision_it_left() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        // What the client read before: the run of the snapshot, and the revision beside the rule.
        let seen = Expects {
            run: RUN.parse()?,
            revision: Revision::new(LEFT.revision.number() - 1),
        };
        let arm = ArmRule {
            environment: "orders-sim".parse()?,
            rule: "delay-1s-on-order-producers".parse()?,
            expects: Some(seen),
        };

        let (answer, request) = tokio::join!(client.send(arm), control_plane.answer(LEFT));

        assert_eq!(answer?, Ok(LEFT));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request?.body)?,
            serde_json::json!({
                "from": { "surface": "api" },
                "operation": "arm-rule",
                "environment": "orders-sim",
                "rule": "delay-1s-on-order-producers",
                "expects": { "run": RUN, "revision": seen.revision.number() },
            }),
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_change_that_expects_another_revision_or_another_run_returns_what_holds_now() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let seen = Expects {
            run: RUN.parse()?,
            revision: Revision::new(LEFT.revision.number() - 1),
        };
        let disarm = DisarmRule {
            environment: "orders-sim".parse()?,
            rule: "delay-1s-on-order-producers".parse()?,
            expects: Some(seen),
        };
        let changed = RuleCommandError::Changed {
            environment: disarm.environment.clone(),
            rule: disarm.rule.clone(),
            revision: LEFT.revision,
        };
        let other_run = RuleCommandError::OtherRun { run: RUN.parse()? };

        let (moved_on, _) = tokio::join!(client.send(disarm.clone()), control_plane.refuse(changed.clone()));
        let (started_again, _) = tokio::join!(client.send(disarm), control_plane.refuse(other_run.clone()));

        // Each is an answer of the operation, which the caller decides on: read the rule again, or the snapshot.
        assert_eq!(moved_on?, Err(changed));
        assert_eq!(started_again?, Err(other_run));
        Ok(())
    }

    #[tokio::test]
    async fn an_operation_that_the_control_plane_does_returns_its_answer() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));

        let (answer, request) = tokio::join!(
            client.send(ListSessions {
                environment: "orders-sim".parse()?
            }),
            control_plane.answer(Vec::<SessionStatus>::new()),
        );

        assert_eq!(answer?, Ok(Vec::new()));
        request?;
        Ok(())
    }

    #[tokio::test]
    async fn an_answer_that_is_not_one_of_the_operation_is_unreadable() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));

        let (answer, request) = tokio::join!(client.send(ListEnvironments {}), control_plane.answer_with_a_web_page(),);

        let error = answer.expect_err("no answer of list-environments");
        assert!(matches!(error, SendError::Unreadable { .. }), "{error}");
        request?;
        Ok(())
    }

    #[tokio::test]
    async fn a_control_plane_that_does_not_listen_is_unreachable() -> anyhow::Result<()> {
        let address = ScriptedControlPlane::start().await?.address()?;
        let client = ControlClient::new(ControlClientConfiguration::new(address));

        let error = client.send(ListEnvironments {}).await.expect_err("nothing listens");

        assert!(matches!(error, SendError::Unreachable { .. }), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn an_operation_that_the_control_plane_does_not_serve_is_rejected() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let keep = KeepRule {
            environment: "orders-sim".parse()?,
            rule: "delay-1s-on-order-producers".parse()?,
            expects: None,
        };

        let (answer, request) = tokio::join!(
            client.send(keep),
            control_plane.reject(RequestError::NotServed {
                operation: "keep-rule".to_owned()
            }),
        );

        assert_eq!(
            answer.expect_err("the control plane does not keep rules yet"),
            SendError::Rejected {
                error: RequestError::NotServed {
                    operation: "keep-rule".to_owned()
                }
            }
        );
        request?;
        Ok(())
    }

    #[tokio::test]
    async fn a_control_plane_that_does_not_answer_within_the_request_timeout_is_unreachable() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let mut configuration = ControlClientConfiguration::new(control_plane.address()?);
        configuration.request_timeout = TimeSpan::from_millis(50);
        let client = ControlClient::new(configuration);

        let (answer, connection) = tokio::join!(client.send(ListEnvironments {}), control_plane.take_without_answer());

        assert_eq!(
            answer.expect_err("no answer"),
            SendError::Unreachable {
                reason: "no answer within 50ms".to_owned()
            }
        );
        drop(connection?);
        Ok(())
    }
}

mod live_stream {
    use super::*;

    #[tokio::test]
    async fn a_stream_starts_with_the_snapshot_and_follows_the_calls_of_its_subscription() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream(TAB));
        let (mut stream, mut server_side) = (stream?, server_side?);
        let every_call = Subscription::Calls {
            environment: "orders-sim".parse()?,
            selector: SelectorRef::Inline(Conditions::new()),
        };

        let snapshot = StreamMessage::Snapshot(ProcessState {
            run: RUN.parse()?,
            definitions: Definitions::default(),
            environments: Vec::new(),
        });
        server_side.send(&snapshot).await?;
        stream.follow(every_call.clone()).await;

        assert_eq!(stream.next().await?, snapshot);
        assert_eq!(server_side.next_subscription().await?, every_call);
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_names_the_presence_of_its_client_and_a_client_that_comes_back_asks_for_it() -> anyhow::Result<()>
    {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));

        // A client that is new asks for no presence, and the control plane opens one for it.
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream(TAB));
        let (stream, server_side) = (stream?, server_side?);
        assert_eq!(server_side.path, "/api/stream");
        assert_eq!(stream.presence(), TAB);

        // The stream breaks. The client comes back as the presence it had, and the control plane still holds it.
        drop((stream, server_side));
        let (again, server_side) = tokio::join!(client.live_stream_as(TAB), control_plane.take_stream(TAB));
        let (again, server_side) = (again?, server_side?);
        assert_eq!(server_side.path, format!("/api/stream?presence={TAB}"));
        assert_eq!(again.presence(), TAB);

        // It comes back once more, too late: the control plane names another presence, and the client knows that
        // the holds of the one it had have ended.
        drop((again, server_side));
        let (late, server_side) = tokio::join!(client.live_stream_as(TAB), control_plane.take_stream(NEW_TAB));
        assert_eq!(late?.presence(), NEW_TAB);
        drop(server_side?);
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_that_does_not_start_with_the_presence_of_its_client_is_unreadable() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));

        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream_without_a_presence());

        let error = stream.err().context("the stream opened without a presence")?;
        assert_eq!(
            error,
            StreamError::Unreadable {
                reason: "the live stream did not start with the presence of its client".to_owned()
            }
        );
        drop(server_side?);
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_that_closes_tells_the_control_plane_and_waits_until_it_let_go() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream(TAB));
        let (stream, server_side) = (stream?, server_side?);

        // The control plane reads that the client closes the stream, and only then lets go of it.
        let ((), told) = tokio::join!(stream.close(), server_side.let_go_once_closed());

        told?;
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_of_a_control_plane_that_stops_ends_and_stays_ended() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream(TAB));
        let (mut stream, mut server_side) = (stream?, server_side?);

        server_side.stop().await?;

        let ended = stream.next().await.expect_err("the stream closed");
        assert_eq!(
            ended,
            StreamError::Unreachable {
                reason: "the control plane stopped".to_owned()
            }
        );
        assert_eq!(stream.next().await.expect_err("the stream stays ended"), ended);
        Ok(())
    }

    #[tokio::test]
    async fn a_message_that_is_none_of_the_live_stream_is_unreadable() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream(TAB));
        let (mut stream, mut server_side) = (stream?, server_side?);

        server_side.send_a_message_of_another_version().await?;

        let error = stream.next().await.expect_err("no message of the live stream");
        assert!(matches!(error, StreamError::Unreadable { .. }), "{error}");
        Ok(())
    }
}

/// A control plane on a free port of this host, which answers what a test tells it to, in the form of the control plane.
///
/// It takes one connection for each answer, and closes it after the answer.
struct ScriptedControlPlane {
    listener: TcpListener,
}

impl ScriptedControlPlane {
    /// The status of an answer, whose body is the answer of the operation.
    const ANSWERED: u16 = 200;
    /// The status of a refusal, whose body is the error of the operation.
    const REFUSED: u16 = 422;
    /// The status of an operation that the control plane does not serve, whose body is a `RequestError`.
    const NOT_SERVED: u16 = 501;
    /// The status of a request that the control plane could not read, whose body is a `RequestError`.
    const NOT_READ: u16 = 400;

    async fn start() -> anyhow::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        Ok(Self { listener })
    }

    fn address(&self) -> anyhow::Result<ControlPlaneAddress> {
        Ok(ControlPlaneAddress::from(self.listener.local_addr()?))
    }

    /// Reads the next request, and answers it with `answer`, the answer of an operation that the control plane did.
    async fn answer<A: Serialize>(&self, answer: A) -> anyhow::Result<ScriptedRequest> {
        self.reply(Self::ANSWERED, &serde_json::to_string(&answer)?).await
    }

    /// Reads the next request, and refuses it with `error`, the error of the operation.
    async fn refuse<E: Serialize>(&self, error: E) -> anyhow::Result<ScriptedRequest> {
        self.reply(Self::REFUSED, &serde_json::to_string(&error)?).await
    }

    /// Reads the next request, and does not take it, for `error`.
    async fn reject(&self, error: RequestError) -> anyhow::Result<ScriptedRequest> {
        let status = match &error {
            RequestError::NotServed { .. } => Self::NOT_SERVED,
            RequestError::Unreadable { .. } => Self::NOT_READ,
        };
        self.reply(status, &serde_json::to_string(&error)?).await
    }

    /// Reads the next request, and answers it as a web server would, which is no control plane.
    async fn answer_with_a_web_page(&self) -> anyhow::Result<ScriptedRequest> {
        self.reply(Self::ANSWERED, "<html><body>Welcome</body></html>").await
    }

    /// Takes the next connection, and returns it to the test, which keeps it open and never answers on it.
    async fn take_without_answer(&self) -> anyhow::Result<TcpStream> {
        let (connection, _) = self.listener.accept().await?;
        Ok(connection)
    }

    /// Takes the next live stream, names `presence` as the presence of its client, and returns the side of the
    /// control plane.
    async fn take_stream(&self, presence: PresenceId) -> anyhow::Result<ScriptedStream> {
        let mut stream = self.accept_stream().await?;
        stream.send(&StreamMessage::Present { presence }).await?;
        Ok(stream)
    }

    /// Takes the next live stream as a control plane that knows no presences, and starts with the snapshot.
    async fn take_stream_without_a_presence(&self) -> anyhow::Result<ScriptedStream> {
        let mut stream = self.accept_stream().await?;
        let snapshot = StreamMessage::Snapshot(ProcessState {
            run: RUN.parse()?,
            definitions: Definitions::default(),
            environments: Vec::new(),
        });
        stream.send(&snapshot).await?;
        Ok(stream)
    }

    /// Takes the next live stream, and sends nothing on it yet.
    async fn accept_stream(&self) -> anyhow::Result<ScriptedStream> {
        let (connection, _) = self.listener.accept().await?;
        let mut path = String::new();
        #[expect(
            clippy::result_large_err,
            reason = "the library names what a handshake answers when it is refused, and this one refuses none"
        )]
        let socket = tokio_tungstenite::accept_hdr_async(connection, |request: &Request, response: Response| {
            path = request.uri().to_string();
            Ok(response)
        })
        .await?;
        Ok(ScriptedStream { socket, path })
    }

    /// Reads the next request, and answers it with `status` and `body`.
    async fn reply(&self, status: u16, body: &str) -> anyhow::Result<ScriptedRequest> {
        let (connection, _) = self.listener.accept().await?;
        let mut connection = BufReader::new(connection);
        let request = ScriptedRequest::read(&mut connection).await?;
        let answer = format!(
            "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len(),
        );
        connection.write_all(answer.as_bytes()).await?;
        connection.shutdown().await?;
        Ok(request)
    }
}

/// The side of the control plane of one live stream, which sends what a test tells it to.
struct ScriptedStream {
    socket: WebSocketStream<TcpStream>,
    /// The path that the client opened the stream at, with its query.
    path: String,
}

impl ScriptedStream {
    async fn send(&mut self, message: &StreamMessage) -> anyhow::Result<()> {
        self.socket.send(Message::text(serde_json::to_string(message)?)).await?;
        Ok(())
    }

    /// Sends a message that a control plane of a later version might send, and that this client does not know.
    async fn send_a_message_of_another_version(&mut self) -> anyhow::Result<()> {
        let message = serde_json::json!({ "message": "weather-changed", "weather": "storm" });
        self.socket.send(Message::text(message.to_string())).await?;
        Ok(())
    }

    /// The next subscription that the client sent.
    async fn next_subscription(&mut self) -> anyhow::Result<Subscription> {
        let message = self.socket.next().await.context("a subscription")??;
        Ok(serde_json::from_str(message.to_text()?)?)
    }

    /// Reads that the client closes the stream, and lets go of the stream.
    async fn let_go_once_closed(mut self) -> anyhow::Result<()> {
        let message = self.socket.next().await.context("a close of the stream")??;
        anyhow::ensure!(message.is_close(), "the client sent {message:?}, and no close");
        Ok(())
    }

    /// Closes the stream as a control plane that stops.
    async fn stop(&mut self) -> anyhow::Result<()> {
        self.socket
            .close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "".into(),
            }))
            .await?;
        Ok(())
    }
}

/// The path and the body of a request that a [`ScriptedControlPlane`] read.
struct ScriptedRequest {
    path: String,
    body: String,
}

impl ScriptedRequest {
    async fn read(connection: &mut BufReader<TcpStream>) -> anyhow::Result<Self> {
        let mut line = String::new();
        connection.read_line(&mut line).await?;
        let path = line.split(' ').nth(1).context("a request line with a path")?.to_owned();
        let mut length = 0;
        loop {
            line.clear();
            connection.read_line(&mut line).await?;
            let header = line.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                length = value.trim().parse()?;
            }
        }
        let mut body = vec![0; length];
        connection.read_exact(&mut body).await?;
        Ok(Self {
            path,
            body: String::from_utf8(body)?,
        })
    }
}
