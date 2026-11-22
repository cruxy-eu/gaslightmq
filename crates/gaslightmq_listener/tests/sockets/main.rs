//! A listener on a real socket: clients connect to a free port, and a handler of the test runs each connection.
//!
//! | File | What it holds |
//! |---|---|
//! | `support` | The client and the handlers of the tests. |
//! | `framing` | Whole frames, however the bytes of the client arrive. |
//! | `closing` | The client, the handler and the listener end a connection. |
//! | `connections` | Connections and listeners side by side, each with its own number. |
//! | `kafka` | A Kafka client gets its answer. |

mod closing;
mod connections;
mod framing;
mod kafka;
mod support;
