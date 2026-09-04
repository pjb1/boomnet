//! Service to manage multiple endpoint lifecycle.

use std::collections::VecDeque;
use std::io;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use crate::service::dns::{BlockingDnsResolver, DnsQuery, DnsResolver};
use crate::service::endpoint::{Context, DisconnectReason, Endpoint, EndpointWithContext};
use crate::service::error::IOServiceOperation;
use crate::service::node::{IONode, IONodes};
use crate::service::select::{Selector, SelectorToken};
use crate::service::time::{SystemTimeClockSource, TimeSource};
use crate::stream::ConnectionInfoProvider;

pub mod dns;
pub mod endpoint;
pub mod error;
mod node;
pub mod select;
pub mod time;

pub use error::IOServiceError;

const ENDPOINT_CREATION_THROTTLE_NS: u64 = Duration::from_secs(1).as_nanos() as u64;

/// Endpoint handle.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
#[repr(transparent)]
pub struct Handle(SelectorToken);

/// Handles the lifecycle of endpoints (see [`Endpoint`]), which are typically network connections.
/// It uses `SelectService` pattern for managing asynchronous I/O operations.
pub struct IOService<S: Selector, E, C, TS, D: DnsResolver> {
    selector: S,
    pending_endpoints: VecDeque<(Handle, D::Query, u64, E)>,
    io_nodes: IONodes<S::Target, E>,
    pending_disconnects: VecDeque<(Handle, DisconnectReason)>,
    next_endpoint_create_time_ns: u64,
    context: PhantomData<C>,
    auto_disconnect: Option<Box<dyn Fn() -> Duration>>,
    time_source: TS,
    dns_resolver: D,
    dns_query_timeout_ns: Option<u64>,
}

/// One unit of work produced by [`IOService::poll`].
#[derive(Debug)]
pub enum IOServiceEvent<E> {
    /// An endpoint became active.
    Connected {
        /// Connected endpoint handle.
        handle: Handle,
    },
    /// An endpoint disconnected and was scheduled for recreation.
    Disconnected {
        /// Disconnected endpoint handle.
        handle: Handle,
        /// Cause of the disconnection.
        reason: DisconnectReason,
    },
    /// Data produced by one endpoint.
    Data {
        /// Endpoint that produced the data.
        handle: Handle,
        /// Endpoint-defined event.
        event: E,
    },
    /// No endpoint produced work during this service poll.
    Idle,
}

/// Defines how an instance that implements `SelectService` can be transformed
/// into an [`IOService`], facilitating the management of asynchronous I/O operations.
pub trait IntoIOService<E> {
    fn into_io_service(self) -> IOService<Self, E, (), SystemTimeClockSource, BlockingDnsResolver>
    where
        Self: Selector,
        Self: Sized;
}

/// Defines how an instance that implements [`Selector`] can be transformed
/// into an [`IOService`] with [`Context`], facilitating the management of asynchronous I/O operations.
pub trait IntoIOServiceWithContext<E, C: Context> {
    fn into_io_service_with_context(self) -> IOService<Self, E, C, SystemTimeClockSource, BlockingDnsResolver>
    where
        Self: Selector,
        Self: Sized;
}

impl<S: Selector, E, C, TS, D: DnsResolver> IOService<S, E, C, TS, D> {
    /// Creates new instance of [`IOService`].
    pub fn new(selector: S, time_source: TS, dns_resolver: D) -> IOService<S, E, C, TS, D> {
        Self {
            selector,
            pending_endpoints: VecDeque::new(),
            io_nodes: IONodes::default(),
            pending_disconnects: VecDeque::new(),
            next_endpoint_create_time_ns: 0,
            context: PhantomData,
            auto_disconnect: None,
            time_source,
            dns_resolver,
            dns_query_timeout_ns: None,
        }
    }

    /// Specify TTL for each [`Endpoint`] connection.
    pub fn with_auto_disconnect(self, auto_disconnect: Duration) -> IOService<S, E, C, TS, D> {
        self.with_auto_disconnect_supplier(move || auto_disconnect)
    }

    /// Specify TTL supplier for each [`Endpoint`] connection.
    pub fn with_auto_disconnect_supplier<F>(self, f: F) -> IOService<S, E, C, TS, D>
    where
        F: Fn() -> Duration + 'static,
    {
        Self {
            auto_disconnect: Some(Box::new(f)),
            ..self
        }
    }

    /// Specify DNS query timeout. This is only relevant when using asynchronous form of
    /// [`DnsResolver`].
    pub fn with_dns_query_timeout(self, timeout: Duration) -> IOService<S, E, C, TS, D> {
        Self {
            dns_query_timeout_ns: Some(timeout.as_nanos() as u64),
            ..self
        }
    }

    /// Specify custom [`TimeSource`] instead of the default system time source.
    pub fn with_time_source<T: TimeSource>(self, time_source: T) -> IOService<S, E, C, T, D> {
        IOService {
            time_source,
            pending_endpoints: Default::default(),
            context: self.context,
            auto_disconnect: self.auto_disconnect,
            io_nodes: Default::default(),
            pending_disconnects: Default::default(),
            next_endpoint_create_time_ns: self.next_endpoint_create_time_ns,
            selector: self.selector,
            dns_resolver: self.dns_resolver,
            dns_query_timeout_ns: self.dns_query_timeout_ns,
        }
    }

    /// Specify custom [`TimeSource`] instead of the default system time source.
    pub fn with_dns_resolver<DR: DnsResolver>(self, dns_resolver: DR) -> IOService<S, E, C, TS, DR> {
        IOService {
            time_source: self.time_source,
            pending_endpoints: Default::default(),
            context: self.context,
            auto_disconnect: self.auto_disconnect,
            io_nodes: Default::default(),
            pending_disconnects: Default::default(),
            next_endpoint_create_time_ns: self.next_endpoint_create_time_ns,
            selector: self.selector,
            dns_resolver,
            dns_query_timeout_ns: self.dns_query_timeout_ns,
        }
    }

    /// Register a new [`Endpoint`] with the service and return a handle to the created endpoint.
    pub fn register(&mut self, endpoint: E) -> Result<Handle, IOServiceError>
    where
        E: ConnectionInfoProvider,
        TS: TimeSource,
    {
        let handle = Handle(self.selector.next_token());
        let info = endpoint.connection_info();
        let query = self
            .dns_resolver
            .new_query(info.host(), info.port())
            .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::Resolve, source))?;
        let now = self.time_source.current_time_nanos();
        self.pending_endpoints.push_back((handle, query, now, endpoint));
        Ok(handle)
    }

    /// Register a new [`Endpoint`] with the service using provided factory and return a handle to
    /// the created endpoint.
    pub fn register_with<F>(&mut self, endpoint_factory: F) -> Result<Handle, IOServiceError>
    where
        E: ConnectionInfoProvider,
        TS: TimeSource,
        F: FnOnce(Handle) -> io::Result<E>,
    {
        let handle = Handle(self.selector.next_token());
        let endpoint = endpoint_factory(handle)
            .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::CreateEndpoint, source))?;
        let info = endpoint.connection_info();
        let query = self
            .dns_resolver
            .new_query(info.host(), info.port())
            .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::Resolve, source))?;
        let now = self.time_source.current_time_nanos();
        self.pending_endpoints.push_back((handle, query, now, endpoint));
        Ok(handle)
    }

    /// Deregister [`Endpoint`] with the service based on a handle.
    pub fn deregister(&mut self, handle: Handle) -> Result<Option<E>, IOServiceError> {
        self.pending_disconnects
            .retain(|(pending_handle, _)| *pending_handle != handle);
        if let Some(io_node) = self.io_nodes.get_mut(handle.0) {
            self.selector
                .unregister(io_node)
                .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::Unregister, source))?;
            match self.io_nodes.remove(handle.0) {
                Some(io_node) => Ok(Some(io_node.into_endpoint().1)),
                None => Err(IOServiceError::InvalidState {
                    handle: Some(handle),
                    message: "endpoint disappeared after selector unregistration",
                }),
            }
        } else {
            let mut index_to_remove = None;
            for (index, endpoint) in self.pending_endpoints.iter().enumerate() {
                if endpoint.0 == handle {
                    index_to_remove = Some(index);
                    break;
                }
            }
            if let Some(index_to_remove) = index_to_remove {
                Ok(self
                    .pending_endpoints
                    .remove(index_to_remove)
                    .map(|(_, _, _, endpoint)| endpoint))
            } else {
                Ok(None)
            }
        }
    }

    /// Return iterator over active endpoints, additionally exposing handle and the stream.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (Handle, &S::Target, &E)> {
        self.io_nodes.values().map(|io_node| {
            let (stream, (handle, endpoint)) = io_node.as_parts();
            (*handle, stream, endpoint)
        })
    }

    /// Return mutable iterator over active endpoints, additionally exposing handle and the stream.
    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (Handle, &mut S::Target, &mut E)> {
        self.io_nodes.values_mut().map(|io_node| {
            let (stream, (handle, endpoint)) = io_node.as_parts_mut();
            (*handle, stream, endpoint)
        })
    }

    /// Return iterator over pending endpoints.
    #[inline]
    pub fn pending(&self) -> impl Iterator<Item = (&Handle, &E)> {
        self.pending_endpoints
            .iter()
            .map(|(handle, _, _, endpoint)| (handle, endpoint))
    }

    #[inline]
    fn resolve_dns(&self, query: &mut impl DnsQuery, created_time_ns: u64) -> io::Result<Option<SocketAddr>>
    where
        TS: TimeSource,
    {
        // check if dns query resolution timed out
        if let Some(dns_query_timeout) = self.dns_query_timeout_ns {
            let now = self.time_source.current_time_nanos();
            if now > created_time_ns + dns_query_timeout {
                return Err(io::Error::new(ErrorKind::TimedOut, "dns resolution timed out"));
            }
        }
        match query.poll() {
            Ok(addrs) => {
                let addr = addrs
                    .into_iter()
                    .next()
                    .ok_or_else(|| io::Error::other("dns resolution dio not return any address"))?;
                Ok(Some(addr))
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(err),
        }
    }

    #[cold]
    fn check_pending_endpoints<F>(&mut self, create_target: F) -> Result<Option<Handle>, IOServiceError>
    where
        E: ConnectionInfoProvider,
        TS: TimeSource,
        F: FnOnce(&mut E, SocketAddr) -> io::Result<Option<<S as Selector>::Target>>,
    {
        let current_time_ns = self.time_source.current_time_nanos();
        if current_time_ns > self.next_endpoint_create_time_ns {
            if let Some((handle, mut query, query_time_ns, mut endpoint)) = self.pending_endpoints.pop_front() {
                if let Some(addr) = self
                    .resolve_dns(&mut query, query_time_ns)
                    .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::Resolve, source))?
                {
                    match create_target(&mut endpoint, addr)
                        .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::CreateTarget, source))?
                    {
                        Some(stream) => {
                            let ttl = self.auto_disconnect.as_ref().map(|auto_disconnect| auto_disconnect());
                            let mut io_node = IONode::new(stream, handle, endpoint, ttl, &self.time_source);
                            self.selector.register(handle.0, &mut io_node).map_err(|source| {
                                IOServiceError::io(Some(handle), IOServiceOperation::Register, source)
                            })?;
                            self.io_nodes
                                .insert(handle.0, io_node)
                                .map_err(|_| IOServiceError::InvalidState {
                                    handle: Some(handle),
                                    message: "endpoint token is already active",
                                })?;
                            self.next_endpoint_create_time_ns = current_time_ns + ENDPOINT_CREATION_THROTTLE_NS;
                            return Ok(Some(handle));
                        }
                        None => {
                            // request new dns query
                            let info = endpoint.connection_info();
                            let query = self
                                .dns_resolver
                                .new_query(info.host(), info.port())
                                .map_err(|source| {
                                    IOServiceError::io(Some(handle), IOServiceOperation::Resolve, source)
                                })?;
                            let now = self.time_source.current_time_nanos();
                            self.pending_endpoints.push_back((handle, query, now, endpoint))
                        }
                    }
                } else {
                    self.pending_endpoints
                        .push_back((handle, query, query_time_ns, endpoint))
                }
            }
            self.next_endpoint_create_time_ns = current_time_ns + ENDPOINT_CREATION_THROTTLE_NS;
        }
        Ok(None)
    }

    #[inline]
    fn next_active_handle(&mut self) -> Option<Handle> {
        self.io_nodes.next_active_token().map(Handle)
    }

    fn remove_active_endpoint(&mut self, handle: Handle) -> Result<E, IOServiceError> {
        let io_node = self.io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
            handle: Some(handle),
            message: "active endpoint is not registered",
        })?;
        self.selector
            .unregister(io_node)
            .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::Unregister, source))?;
        let io_node = self.io_nodes.remove(handle.0).ok_or(IOServiceError::InvalidState {
            handle: Some(handle),
            message: "endpoint disappeared after selector unregistration",
        })?;
        Ok(io_node.into_endpoint().1)
    }

    fn schedule_reconnect(&mut self, handle: Handle, endpoint: E) -> Result<(), IOServiceError>
    where
        E: ConnectionInfoProvider,
        TS: TimeSource,
    {
        let info = endpoint.connection_info();
        let query = self
            .dns_resolver
            .new_query(info.host(), info.port())
            .map_err(|source| IOServiceError::io(Some(handle), IOServiceOperation::Resolve, source))?;
        let now = self.time_source.current_time_nanos();
        self.pending_endpoints.push_back((handle, query, now, endpoint));
        Ok(())
    }
}

impl<S, E, TS, D> IOService<S, E, (), TS, D>
where
    S: Selector,
    E: Endpoint<Target = S::Target>,
    TS: TimeSource,
    D: DnsResolver,
{
    /// Advance the service once and return at most one fair unit of endpoint work.
    ///
    /// Active endpoints are selected in persistent round-robin order. The returned event may
    /// borrow its endpoint target and therefore must be dropped before polling the service again.
    pub fn poll(&mut self) -> Result<IOServiceEvent<E::Event<'_>>, IOServiceError> {
        if let Some((handle, reason)) = self.pending_disconnects.pop_front() {
            let can_recreate = self
                .io_nodes
                .get_mut(handle.0)
                .ok_or(IOServiceError::InvalidState {
                    handle: Some(handle),
                    message: "disconnected endpoint is not registered",
                })?
                .as_endpoint_mut()
                .1
                .can_recreate(&reason);
            let endpoint = self.remove_active_endpoint(handle)?;
            if !can_recreate {
                return Err(IOServiceError::EndpointNotRecreatable { handle, reason });
            }
            self.schedule_reconnect(handle, endpoint)?;
            return Ok(IOServiceEvent::Disconnected { handle, reason });
        }

        if !self.pending_endpoints.is_empty()
            && let Some(handle) = self.check_pending_endpoints(|endpoint, addr| endpoint.create_target(addr))?
        {
            return Ok(IOServiceEvent::Connected { handle });
        }

        self.selector
            .poll(&mut self.io_nodes)
            .map_err(|source| IOServiceError::io(None, IOServiceOperation::PollSelector, source))?;

        let Some(handle) = self.next_active_handle() else {
            return Ok(IOServiceEvent::Idle);
        };

        let auto_disconnect = self.auto_disconnect.as_ref();
        if let Some(auto_disconnect) = auto_disconnect {
            let current_time_ns = self.time_source.current_time_nanos();
            let force_disconnect = self
                .io_nodes
                .get(handle.0)
                .is_some_and(|node| current_time_ns > node.disconnect_time_ns);
            if force_disconnect {
                let can_auto_disconnect = self
                    .io_nodes
                    .get_mut(handle.0)
                    .ok_or(IOServiceError::InvalidState {
                        handle: Some(handle),
                        message: "active endpoint is not registered",
                    })?
                    .as_endpoint_mut()
                    .1
                    .can_auto_disconnect();
                if can_auto_disconnect {
                    let node = self.io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
                        handle: Some(handle),
                        message: "active endpoint is not registered",
                    })?;
                    let ttl = node.ttl;
                    let reason = DisconnectReason::auto_disconnect(ttl);
                    let can_recreate = node.as_endpoint_mut().1.can_recreate(&reason);
                    let endpoint = self.remove_active_endpoint(handle)?;
                    if !can_recreate {
                        return Err(IOServiceError::EndpointNotRecreatable { handle, reason });
                    }
                    self.schedule_reconnect(handle, endpoint)?;
                    return Ok(IOServiceEvent::Disconnected { handle, reason });
                }

                let extend = auto_disconnect().as_nanos() as u64;
                let node = self.io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
                    handle: Some(handle),
                    message: "active endpoint is not registered",
                })?;
                node.disconnect_time_ns = node.disconnect_time_ns.saturating_add(extend);
            }
        }

        let (io_nodes, pending_disconnects) = (&mut self.io_nodes, &mut self.pending_disconnects);
        let result = {
            let node = io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
                handle: Some(handle),
                message: "active endpoint is not registered",
            })?;
            let (target, (_, endpoint)) = node.as_parts_mut();
            endpoint.poll(target)
        };

        match result {
            Ok(Some(event)) => Ok(IOServiceEvent::Data { handle, event }),
            Ok(None) => Ok(IOServiceEvent::Idle),
            Err(source) => {
                pending_disconnects.push_back((handle, DisconnectReason::other(source)));
                Ok(IOServiceEvent::Idle)
            }
        }
    }

    /// Dispatch command to an active endpoint using `handle` and provided `action`. If the
    /// endpoint is currently active `Ok(Some(...))` will be returned and the provided `action` invoked,
    /// otherwise this method will return `Ok(None)` and no `action` will be invoked.
    pub fn dispatch<F, T>(&mut self, handle: Handle, mut action: F) -> io::Result<Option<T>>
    where
        F: FnMut(&mut E::Target, &mut E) -> std::io::Result<T>,
    {
        match self.io_nodes.get_mut(handle.0) {
            Some(io_node) => {
                let (stream, (_, endpoint)) = io_node.as_parts_mut();
                let result = action(stream, endpoint)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

impl<S, E, C, TS, D> IOService<S, E, C, TS, D>
where
    S: Selector,
    C: Context,
    E: EndpointWithContext<C, Target = S::Target>,
    TS: TimeSource,
    D: DnsResolver,
{
    /// Advance the service once and return at most one fair unit of endpoint work.
    ///
    /// Active endpoints are selected in persistent round-robin order. The returned event may
    /// borrow its endpoint target or context and therefore must be dropped before polling again.
    pub fn poll<'a>(&'a mut self, ctx: &'a mut C) -> Result<IOServiceEvent<E::Event<'a>>, IOServiceError> {
        if let Some((handle, reason)) = self.pending_disconnects.pop_front() {
            let can_recreate = self
                .io_nodes
                .get_mut(handle.0)
                .ok_or(IOServiceError::InvalidState {
                    handle: Some(handle),
                    message: "disconnected endpoint is not registered",
                })?
                .as_endpoint_mut()
                .1
                .can_recreate(&reason, ctx);
            let endpoint = self.remove_active_endpoint(handle)?;
            if !can_recreate {
                return Err(IOServiceError::EndpointNotRecreatable { handle, reason });
            }
            self.schedule_reconnect(handle, endpoint)?;
            return Ok(IOServiceEvent::Disconnected { handle, reason });
        }

        if !self.pending_endpoints.is_empty()
            && let Some(handle) = self.check_pending_endpoints(|endpoint, addr| endpoint.create_target(addr, ctx))?
        {
            return Ok(IOServiceEvent::Connected { handle });
        }

        self.selector
            .poll(&mut self.io_nodes)
            .map_err(|source| IOServiceError::io(None, IOServiceOperation::PollSelector, source))?;

        let Some(handle) = self.next_active_handle() else {
            return Ok(IOServiceEvent::Idle);
        };

        let auto_disconnect = self.auto_disconnect.as_ref();
        if let Some(auto_disconnect) = auto_disconnect {
            let current_time_ns = self.time_source.current_time_nanos();
            let force_disconnect = self
                .io_nodes
                .get(handle.0)
                .is_some_and(|node| current_time_ns > node.disconnect_time_ns);
            if force_disconnect {
                let can_auto_disconnect = self
                    .io_nodes
                    .get_mut(handle.0)
                    .ok_or(IOServiceError::InvalidState {
                        handle: Some(handle),
                        message: "active endpoint is not registered",
                    })?
                    .as_endpoint_mut()
                    .1
                    .can_auto_disconnect(ctx);
                if can_auto_disconnect {
                    let node = self.io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
                        handle: Some(handle),
                        message: "active endpoint is not registered",
                    })?;
                    let ttl = node.ttl;
                    let reason = DisconnectReason::auto_disconnect(ttl);
                    let can_recreate = node.as_endpoint_mut().1.can_recreate(&reason, ctx);
                    let endpoint = self.remove_active_endpoint(handle)?;
                    if !can_recreate {
                        return Err(IOServiceError::EndpointNotRecreatable { handle, reason });
                    }
                    self.schedule_reconnect(handle, endpoint)?;
                    return Ok(IOServiceEvent::Disconnected { handle, reason });
                }

                let extend = auto_disconnect().as_nanos() as u64;
                let node = self.io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
                    handle: Some(handle),
                    message: "active endpoint is not registered",
                })?;
                node.disconnect_time_ns = node.disconnect_time_ns.saturating_add(extend);
            }
        }

        let (io_nodes, pending_disconnects) = (&mut self.io_nodes, &mut self.pending_disconnects);
        let result = {
            let node = io_nodes.get_mut(handle.0).ok_or(IOServiceError::InvalidState {
                handle: Some(handle),
                message: "active endpoint is not registered",
            })?;
            let (target, (_, endpoint)) = node.as_parts_mut();
            endpoint.poll(target, ctx)
        };

        match result {
            Ok(Some(event)) => Ok(IOServiceEvent::Data { handle, event }),
            Ok(None) => Ok(IOServiceEvent::Idle),
            Err(source) => {
                pending_disconnects.push_back((handle, DisconnectReason::other(source)));
                Ok(IOServiceEvent::Idle)
            }
        }
    }

    /// Dispatch command to an active endpoint using `handle` and provided `action`. If the
    /// endpoint is currently active `Ok(Some(...))` will be returned and the provided `action` invoked,
    /// otherwise this method will return `Ok(None)` and no `action` will be invoked. This method
    /// requires `Context` to be passed and exposes it to the provided `action`.
    pub fn dispatch<F, T>(&mut self, handle: Handle, ctx: &mut C, mut action: F) -> io::Result<Option<T>>
    where
        F: FnMut(&mut E::Target, &mut E, &mut C) -> std::io::Result<T>,
    {
        match self.io_nodes.get_mut(handle.0) {
            Some(io_node) => {
                let (stream, (_, endpoint)) = io_node.as_parts_mut();
                let result = action(stream, endpoint, ctx)?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::dns::{DnsQuery, DnsResolver};
    use crate::service::select::Selectable;
    use std::cell::Cell;
    use std::rc::Rc;

    struct TestTarget;

    impl Selectable for TestTarget {
        fn connected(&mut self) -> io::Result<bool> {
            Ok(true)
        }

        fn make_writable(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn make_readable(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestSelector {
        next_token: SelectorToken,
    }

    impl Selector for TestSelector {
        type Target = TestTarget;

        fn register<E>(&mut self, _token: SelectorToken, _node: &mut IONode<Self::Target, E>) -> io::Result<()> {
            Ok(())
        }

        fn unregister<E>(&mut self, _node: &mut IONode<Self::Target, E>) -> io::Result<()> {
            Ok(())
        }

        fn poll<E>(&mut self, _nodes: &mut IONodes<Self::Target, E>) -> io::Result<()> {
            Ok(())
        }

        fn next_token(&mut self) -> SelectorToken {
            let token = self.next_token;
            self.next_token += 1;
            token
        }
    }

    struct FixedDns;
    struct FixedQuery;

    impl DnsResolver for FixedDns {
        type Query = FixedQuery;

        fn new_query(&self, _host: impl AsRef<str>, _port: u16) -> io::Result<Self::Query> {
            Ok(FixedQuery)
        }
    }

    impl DnsQuery for FixedQuery {
        fn poll(&mut self) -> io::Result<impl IntoIterator<Item = SocketAddr>> {
            Ok([SocketAddr::from(([127, 0, 0, 1], 1234))])
        }
    }

    #[derive(Clone)]
    struct ManualTime(Rc<Cell<u64>>);

    impl TimeSource for ManualTime {
        fn current_time_nanos(&self) -> u64 {
            self.0.get()
        }
    }

    struct TestEndpoint {
        id: u32,
        connection_info: crate::stream::ConnectionInfo,
        fail_poll: bool,
        recreate: bool,
    }

    impl TestEndpoint {
        fn new(id: u32) -> Self {
            Self {
                id,
                connection_info: crate::stream::ConnectionInfo::new("localhost", 1234),
                fail_poll: false,
                recreate: true,
            }
        }

        fn terminal(id: u32) -> Self {
            Self {
                fail_poll: true,
                recreate: false,
                ..Self::new(id)
            }
        }
    }

    impl ConnectionInfoProvider for TestEndpoint {
        fn connection_info(&self) -> &crate::stream::ConnectionInfo {
            &self.connection_info
        }
    }

    impl Endpoint for TestEndpoint {
        type Target = TestTarget;
        type Event<'a> = u32;

        fn create_target(&mut self, _addr: SocketAddr) -> io::Result<Option<Self::Target>> {
            Ok(Some(TestTarget))
        }

        fn poll<'a>(&'a mut self, _target: &'a mut Self::Target) -> io::Result<Option<Self::Event<'a>>> {
            if self.fail_poll {
                Err(io::Error::new(ErrorKind::ConnectionReset, "test disconnect"))
            } else {
                Ok(Some(self.id))
            }
        }

        fn can_recreate(&mut self, _reason: &DisconnectReason) -> bool {
            self.recreate
        }
    }

    fn service(time: ManualTime) -> IOService<TestSelector, TestEndpoint, (), ManualTime, FixedDns> {
        IOService::new(TestSelector::default(), time, FixedDns)
    }

    fn connect_next(
        service: &mut IOService<TestSelector, TestEndpoint, (), ManualTime, FixedDns>,
        now: &Rc<Cell<u64>>,
        time_ns: u64,
    ) {
        now.set(time_ns);
        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Connected { .. }));
    }

    #[test]
    fn polls_active_endpoints_in_round_robin_order() {
        let now = Rc::new(Cell::new(1));
        let mut service = service(ManualTime(now.clone()));
        for id in 0..3 {
            service.register(TestEndpoint::new(id)).unwrap();
        }

        connect_next(&mut service, &now, 1);
        connect_next(&mut service, &now, 1_000_000_002);
        connect_next(&mut service, &now, 2_000_000_003);

        let mut events = Vec::new();
        for _ in 0..4 {
            match service.poll().unwrap() {
                IOServiceEvent::Data { event, .. } => events.push(event),
                _ => panic!("expected a batch"),
            }
        }
        assert_eq!(events, [0, 1, 2, 0]);
    }

    #[test]
    fn round_robin_skips_deregistered_slots() {
        let now = Rc::new(Cell::new(1));
        let mut service = service(ManualTime(now.clone()));
        let handles = (0..3)
            .map(|id| service.register(TestEndpoint::new(id)).unwrap())
            .collect::<Vec<_>>();

        connect_next(&mut service, &now, 1);
        connect_next(&mut service, &now, 1_000_000_002);
        connect_next(&mut service, &now, 2_000_000_003);

        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Data { event: 0, .. }));
        service.deregister(handles[1]).unwrap();
        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Data { event: 2, .. }));
        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Data { event: 0, .. }));
    }

    #[test]
    fn returns_error_when_disconnected_endpoint_declines_recreation() {
        let now = Rc::new(Cell::new(1));
        let mut service = service(ManualTime(now.clone()));
        let handle = service.register(TestEndpoint::terminal(7)).unwrap();
        connect_next(&mut service, &now, 1);

        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Idle));
        let error = service.poll().unwrap_err();
        match error {
            IOServiceError::EndpointNotRecreatable {
                handle: error_handle,
                reason: DisconnectReason::IO(source),
            } => {
                assert_eq!(error_handle, handle);
                assert_eq!(source.kind(), ErrorKind::ConnectionReset);
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(service.iter().count(), 0);
    }

    #[test]
    fn deregister_clears_a_queued_disconnect() {
        let now = Rc::new(Cell::new(1));
        let mut service = service(ManualTime(now.clone()));
        let handle = service.register(TestEndpoint::terminal(7)).unwrap();
        connect_next(&mut service, &now, 1);

        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Idle));
        assert!(service.deregister(handle).unwrap().is_some());
        assert!(matches!(service.poll().unwrap(), IOServiceEvent::Idle));
    }
}
