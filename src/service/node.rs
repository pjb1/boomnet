use crate::service::Handle;
use crate::service::select::SelectorToken;
use crate::service::time::TimeSource;
use std::time::Duration;

pub struct IONode<S, E> {
    pub stream: S,
    pub endpoint: (Handle, E),
    pub ttl: Duration,
    pub disconnect_time_ns: u64,
}

/// Token-indexed storage for active I/O nodes.
pub struct IONodes<S, E> {
    slots: Vec<Option<IONode<S, E>>>,
    next_poll_index: usize,
    active_count: usize,
}

const MIN_IO_NODE_SLOTS: usize = 4;

impl<S, E> Default for IONodes<S, E> {
    fn default() -> Self {
        Self {
            slots: std::iter::repeat_with(|| None).take(MIN_IO_NODE_SLOTS).collect(),
            next_poll_index: 0,
            active_count: 0,
        }
    }
}

impl<S, E> IONodes<S, E> {
    pub fn insert(&mut self, token: SelectorToken, node: IONode<S, E>) -> Result<(), IONode<S, E>> {
        let index = token as usize;
        if index >= self.slots.len() {
            let new_len = (index + 1).next_power_of_two().max(MIN_IO_NODE_SLOTS);
            self.slots.resize_with(new_len, || None);
        }
        let slot = &mut self.slots[index];
        if slot.is_some() {
            return Err(node);
        }
        *slot = Some(node);
        self.active_count += 1;
        Ok(())
    }

    #[inline]
    pub fn get(&self, token: SelectorToken) -> Option<&IONode<S, E>> {
        self.slots.get(token as usize)?.as_ref()
    }

    #[inline]
    pub fn get_mut(&mut self, token: SelectorToken) -> Option<&mut IONode<S, E>> {
        self.slots.get_mut(token as usize)?.as_mut()
    }

    pub fn remove(&mut self, token: SelectorToken) -> Option<IONode<S, E>> {
        let node = self.slots.get_mut(token as usize)?.take()?;
        self.active_count -= 1;
        let required_len = self
            .slots
            .iter()
            .rposition(Option::is_some)
            .map_or(0, |index| index + 1);
        let new_len = required_len.next_power_of_two().max(MIN_IO_NODE_SLOTS);
        if new_len < self.slots.len() {
            self.slots.truncate(new_len);
        }
        self.next_poll_index &= self.slots.len() - 1;
        Some(node)
    }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &IONode<S, E>> {
        self.slots.iter().flatten()
    }

    #[inline]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut IONode<S, E>> {
        self.slots.iter_mut().flatten()
    }

    #[inline]
    pub fn next_active_token(&mut self) -> Option<SelectorToken> {
        if self.active_count == 0 {
            return None;
        }

        for _ in 0..self.slots.len() {
            let index = self.next_poll_index;
            self.next_poll_index = (index + 1) & (self.slots.len() - 1);
            if self.slots[index].is_some() {
                return Some(index as SelectorToken);
            }
        }
        None
    }
}

impl<S, E> IONode<S, E> {
    pub fn new<TS>(stream: S, handle: Handle, endpoint: E, ttl: Option<Duration>, ts: &TS) -> IONode<S, E>
    where
        TS: TimeSource,
    {
        let ttl = ttl.map_or(u64::MAX, |ttl| ttl.as_nanos() as u64);
        Self {
            stream,
            endpoint: (handle, endpoint),
            ttl: Duration::from_nanos(ttl),
            disconnect_time_ns: ts.current_time_nanos().saturating_add(ttl),
        }
    }

    #[inline]
    pub const fn as_parts(&self) -> (&S, &(Handle, E)) {
        (&self.stream, &self.endpoint)
    }

    #[inline]
    pub const fn as_parts_mut(&mut self) -> (&mut S, &mut (Handle, E)) {
        (&mut self.stream, &mut self.endpoint)
    }

    #[inline]
    pub const fn as_stream(&self) -> &S {
        &self.stream
    }

    #[inline]
    pub const fn as_stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    #[inline]
    pub const fn as_endpoint(&self) -> &(Handle, E) {
        &self.endpoint
    }

    #[inline]
    pub const fn as_endpoint_mut(&mut self) -> &mut (Handle, E) {
        &mut self.endpoint
    }

    #[inline]
    pub fn into_endpoint(self) -> (Handle, E) {
        self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedTime;

    impl TimeSource for FixedTime {
        fn current_time_nanos(&self) -> u64 {
            0
        }
    }

    #[test]
    fn storage_size_remains_a_power_of_two() {
        let mut nodes = IONodes::<(), ()>::default();
        assert_eq!(nodes.slots.len(), MIN_IO_NODE_SLOTS);

        let node = IONode::new((), Handle(4), (), None, &FixedTime);
        assert!(nodes.insert(4, node).is_ok());
        assert_eq!(nodes.slots.len(), 8);
        assert_eq!(nodes.next_active_token(), Some(4));
        assert_eq!(nodes.next_active_token(), Some(4));

        assert!(nodes.remove(4).is_some());
        assert_eq!(nodes.slots.len(), MIN_IO_NODE_SLOTS);
        assert!(nodes.slots.len().is_power_of_two());
        assert_eq!(nodes.next_active_token(), None);
    }
}
