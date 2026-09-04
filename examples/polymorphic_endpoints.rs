#![allow(unused)]

use std::io;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use boomnet::inet::{IntoNetworkInterface, ToSocketAddr};
use boomnet::service::endpoint::{Context, Endpoint};
use boomnet::service::select::mio::MioSelector;
use boomnet::service::{IOServiceEvent, IntoIOService, IntoIOServiceWithContext};
use boomnet::stream::mio::{IntoMioStream, MioStream};
use boomnet::stream::tls::TlsStream;
use boomnet::stream::{BindAndConnect, ConnectionInfo, ConnectionInfoProvider};
use boomnet::ws::{IntoTlsWebsocket, Websocket, WebsocketFrame};
use idle::IdleStrategy;
use log::info;
use url::Url;

enum MarketDataEndpoint {
    Trade(TradeEndpoint),
    Ticker(TickerEndpoint),
}

impl ConnectionInfoProvider for MarketDataEndpoint {
    fn connection_info(&self) -> &ConnectionInfo {
        match self {
            MarketDataEndpoint::Ticker(ticker) => ticker.connection_info(),
            MarketDataEndpoint::Trade(trade) => trade.connection_info(),
        }
    }
}

impl Endpoint for MarketDataEndpoint {
    type Target = Websocket<TlsStream<MioStream>>;

    fn create_target(&mut self, addr: SocketAddr) -> io::Result<Option<Self::Target>> {
        match self {
            MarketDataEndpoint::Ticker(ticker) => ticker.create_target(addr),
            MarketDataEndpoint::Trade(trade) => trade.create_target(addr),
        }
    }
}

struct TradeEndpoint {
    id: u32,
    connection_info: ConnectionInfo,
    instrument: &'static str,
}

impl TradeEndpoint {
    pub fn new(id: u32, url: &'static str, instrument: &'static str) -> TradeEndpoint {
        let connection_info = Url::parse(url).try_into().unwrap();
        Self {
            id,
            connection_info,
            instrument,
        }
    }
}

impl ConnectionInfoProvider for TradeEndpoint {
    fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }
}

impl Endpoint for TradeEndpoint {
    type Target = Websocket<TlsStream<MioStream>>;

    fn create_target(&mut self, addr: SocketAddr) -> io::Result<Option<Self::Target>> {
        let mut ws = self
            .connection_info
            .clone()
            .into_tcp_stream_with_addr(addr)?
            .into_mio_stream()
            .into_tls_websocket("/ws")?;

        ws.send_text(
            true,
            Some(format!(r#"{{"method":"SUBSCRIBE","params":["{}@trade"],"id":1}}"#, self.instrument).as_bytes()),
        )?;

        Ok(Some(ws))
    }
}

struct TickerEndpoint {
    id: u32,
    connection_info: ConnectionInfo,
    instrument: &'static str,
}

impl TickerEndpoint {
    pub fn new(id: u32, url: &'static str, instrument: &'static str) -> TickerEndpoint {
        let connection_info = Url::parse(url).try_into().unwrap();
        Self {
            id,
            connection_info,
            instrument,
        }
    }
}

impl ConnectionInfoProvider for TickerEndpoint {
    fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }
}

impl Endpoint for TickerEndpoint {
    type Target = Websocket<TlsStream<MioStream>>;

    fn create_target(&mut self, addr: SocketAddr) -> io::Result<Option<Self::Target>> {
        let mut ws = self
            .connection_info
            .clone()
            .into_tcp_stream_with_addr(addr)?
            .into_mio_stream()
            .into_tls_websocket("/ws")?;

        ws.send_text(
            true,
            Some(format!(r#"{{"method":"SUBSCRIBE","params":["{}@ticker"],"id":1}}"#, self.instrument).as_bytes()),
        )?;

        Ok(Some(ws))
    }
}

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut io_service = MioSelector::new()?.into_io_service();

    let ticker = MarketDataEndpoint::Ticker(TickerEndpoint::new(0, "wss://stream.binance.com:443/ws", "btcusdt"));
    let trade = MarketDataEndpoint::Trade(TradeEndpoint::new(1, "wss://stream.binance.com:443/ws", "ethusdt"));

    let ticker_handle = io_service.register(ticker)?;
    let trade_handle = io_service.register(trade)?;

    loop {
        for event in io_service.poll()? {
            if let IOServiceEvent::Active(active) = event {
                let label = match active.handle() {
                    handle if handle == trade_handle => "TRADE",
                    handle if handle == ticker_handle => "TICKER",
                    _ => unreachable!("unknown endpoint handle"),
                };
                let batch = active.try_with(|ws| {
                    ws.read_batch()
                        .map(|batch| batch.into_iter().map(|frame| frame.map_err(io::Error::from)))
                        .map_err(io::Error::from)
                })?;
                for frame in batch {
                    if let WebsocketFrame::Text(fin, data) = frame? {
                        info!("[{label}] ({fin}) {}", String::from_utf8_lossy(data));
                    }
                }
            }
        }
    }
}
