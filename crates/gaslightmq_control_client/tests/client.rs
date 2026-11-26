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
use gaslightmq_control_client::{
    ControlClient, ControlClientConfiguration, ControlPlaneAddress, SendError, StreamError,
};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

mod operations {
    use super::*;

    #[tokio::test]
    async fn an_operation_that_the_control_plane_refuses_returns_its_own_error() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let arm = ArmRule {
            environment: "orders-sim".parse()?,
            rule: "delay-1s-on-order-producers".parse()?,
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
        };
        let disarm = DisarmRule {
            environment: arm.environment.clone(),
            rule: arm.rule.clone(),
        };
        let undo = RequestSource::new(RequestSurface::Ui).with_cause(RequestCause::Undo);

        let (_, from_a_view) = tokio::join!(ui.send(arm.clone()), control_plane.answer(()));
        let (_, from_a_pad) = tokio::join!(ui.send_from(RequestSurface::Pad, arm.clone()), control_plane.answer(()));
        let (_, taken_back) = tokio::join!(ui.send_from(undo, disarm.clone()), control_plane.answer(()));

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
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream());
        let (mut stream, mut server_side) = (stream?, server_side?);
        let every_call = Subscription::Calls {
            environment: "orders-sim".parse()?,
            selector: SelectorRef::Inline(Conditions::new()),
        };

        let snapshot = StreamMessage::Snapshot(ProcessState::default());
        server_side.send(&snapshot).await?;
        stream.follow(every_call.clone()).await;

        assert_eq!(stream.next().await?, snapshot);
        assert_eq!(server_side.next_subscription().await?, every_call);
        Ok(())
    }

    #[tokio::test]
    async fn a_stream_of_a_control_plane_that_stops_ends_and_stays_ended() -> anyhow::Result<()> {
        let control_plane = ScriptedControlPlane::start().await?;
        let client = ControlClient::new(ControlClientConfiguration::new(control_plane.address()?));
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream());
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
        let (stream, server_side) = tokio::join!(client.live_stream(), control_plane.take_stream());
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

    /// Takes the next live stream, and returns the side of the control plane.
    async fn take_stream(&self) -> anyhow::Result<ScriptedStream> {
        let (connection, _) = self.listener.accept().await?;
        Ok(ScriptedStream {
            socket: tokio_tungstenite::accept_async(connection).await?,
        })
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
