use boomnet::service::endpoint::{Context, EndpointWithContext};
use boomnet::stream::buffer::{BufferedStream, IntoBufferedStream};
use boomnet::stream::tcp::TcpStream;
use boomnet::stream::{ConnectionInfo, ConnectionInfoProvider};
use boomnet::ws::{BatchIter, IntoWebsocket, Websocket, WebsocketFrame};
use std::net::SocketAddr;

pub struct TestContext {
    pub wants_write: bool,
    pub processed: usize,
}

impl Context for TestContext {}

impl TestContext {
    pub fn new() -> TestContext {
        Self {
            wants_write: true,
            processed: 0,
        }
    }
}

pub struct TestEndpoint {
    connection_info: ConnectionInfo,
    payload: &'static str,
}

pub struct TestBatch<'a> {
    frames: BatchIter<'a, BufferedStream<TcpStream>>,
    processed: &'a mut usize,
}

impl Iterator for TestBatch<'_> {
    type Item = Result<WebsocketFrame, boomnet::ws::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let frame = self.frames.next()?;
        if frame.is_ok() {
            *self.processed += 1;
        }
        Some(frame)
    }
}

impl ConnectionInfoProvider for TestEndpoint {
    fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }
}

impl EndpointWithContext<TestContext> for TestEndpoint {
    type Target = Websocket<BufferedStream<TcpStream>>;
    type Event<'a> = TestBatch<'a>;

    fn create_target(&mut self, addr: SocketAddr, _ctx: &mut TestContext) -> std::io::Result<Option<Self::Target>> {
        let ws = self
            .connection_info
            .clone()
            .into_tcp_stream_with_addr(addr)?
            .into_default_buffered_stream()
            .into_websocket("/");
        Ok(Some(ws))
    }

    fn poll<'a>(
        &'a mut self,
        ws: &'a mut Self::Target,
        ctx: &'a mut TestContext,
    ) -> std::io::Result<Option<Self::Event<'a>>> {
        if ctx.wants_write {
            ws.send_text(true, Some(self.payload.as_bytes()))?;
            ctx.wants_write = false;
            Ok(None)
        } else {
            Ok(Some(TestBatch {
                frames: ws.read_batch()?.into_iter(),
                processed: &mut ctx.processed,
            }))
        }
    }
}

impl TestEndpoint {
    pub fn new(port: u16, payload: &'static str) -> Self {
        Self {
            connection_info: ConnectionInfo::new("127.0.0.1", port),
            payload,
        }
    }
}
