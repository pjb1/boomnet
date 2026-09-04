//! Entry point for the application logic.

use crate::stream::ConnectionInfoProvider;
use std::fmt::{Debug, Display};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

/// Entry point for the application logic. Endpoints are registered and Managed by 'IOService'.
pub trait Endpoint: ConnectionInfoProvider {
    /// Defines protocol and stream this endpoint operates on.
    type Target;

    /// Event produced while polling the endpoint.
    type Event<'a>
    where
        Self: 'a,
        Self::Target: 'a;

    /// Used by the `IOService` to create connection upon disconnect by passing resolved `addr`.
    /// If the endpoint does not want to connect at this stage it should return `Ok(None)` and
    /// await the next connection attempt with (possibly) different `addr`.
    fn create_target(&mut self, addr: SocketAddr) -> io::Result<Option<Self::Target>>;

    /// Poll the active target for the next available event.
    ///
    /// `Ok(None)` means the selected endpoint currently has no event. An error begins the
    /// disconnect/recreation lifecycle and is passed to [`Endpoint::can_recreate`].
    fn poll<'a>(&'a mut self, target: &'a mut Self::Target) -> io::Result<Option<Self::Event<'a>>>;

    /// Upon disconnection `IOService` will query the endpoint if the connection should be
    /// recreated, passing the disconnect `reason`. Returning `false` makes the service return
    /// [`crate::service::IOServiceError::EndpointNotRecreatable`].
    fn can_recreate(&mut self, _reason: &DisconnectReason) -> bool {
        true
    }

    /// When `auto_disconnect` is used the service will check with the endpoint before
    /// disconnecting. If `false` is returned the service will update the endpoint next
    /// disconnect time as per the `auto_disconnect` configuration.
    fn can_auto_disconnect(&mut self) -> bool {
        true
    }
}

/// Marker trait to be applied on user defined `struct` that is registered with 'IOService'
/// as context.
pub trait Context {}

/// Entry point for the application logic that exposes user provided [Context].
/// Endpoints are registered and Managed by `IOService`.
pub trait EndpointWithContext<C>: ConnectionInfoProvider {
    /// Defines protocol and stream this endpoint operates on.
    type Target;

    /// Event produced while polling the endpoint.
    type Event<'a>
    where
        Self: 'a,
        Self::Target: 'a,
        C: 'a;

    /// Used by the `IOService` to create connection upon disconnect passing resolved `addr` and
    /// user provided `Context`. If the endpoint does not want to connect at this stage it should
    /// return `Ok(None)` and await the next connection attempt with (possibly) different `addr`.
    fn create_target(&mut self, addr: SocketAddr, context: &mut C) -> io::Result<Option<Self::Target>>;

    /// Poll the active target for the next available event.
    ///
    /// `Ok(None)` means the selected endpoint currently has no event. An error begins the
    /// disconnect/recreation lifecycle and is passed to [`EndpointWithContext::can_recreate`].
    fn poll<'a>(&'a mut self, target: &'a mut Self::Target, context: &'a mut C) -> io::Result<Option<Self::Event<'a>>>;

    /// Upon disconnection `IOService` will query the endpoint if the connection should be
    /// recreated, passing the disconnect `reason`. Returning `false` makes the service return
    /// [`crate::service::IOServiceError::EndpointNotRecreatable`].
    fn can_recreate(&mut self, _reason: &DisconnectReason, _context: &mut C) -> bool {
        true
    }

    /// When `auto_disconnect` is used the service will check with the endpoint before
    /// disconnecting. If `false` is returned the service will update the endpoint next
    /// disconnect time as per the `auto_disconnect` configuration.
    fn can_auto_disconnect(&mut self, _context: &mut C) -> bool {
        true
    }
}

/// Disconnect reason passed into `can_recreate()` service call.
#[derive(Debug)]
pub enum DisconnectReason {
    /// This is expected disconnection due to `ttl` on the connection expiring.
    AutoDisconnect(Duration),
    /// IO error has occurred such as reaching EOF or peer disconnect.
    IO(io::Error),
}

impl Display for DisconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisconnectReason::AutoDisconnect(ttl) => {
                write!(f, "auto-disconnect after ")?;
                ttl.fmt(f)
            }
            DisconnectReason::IO(err) => {
                write!(f, "{err}")
            }
        }
    }
}

impl DisconnectReason {
    pub(crate) fn auto_disconnect(ttl: Duration) -> DisconnectReason {
        DisconnectReason::AutoDisconnect(ttl)
    }

    pub(crate) fn other(err: io::Error) -> DisconnectReason {
        DisconnectReason::IO(err)
    }
}
