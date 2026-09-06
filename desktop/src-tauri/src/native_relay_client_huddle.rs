//! Huddle consumer for the shared native relay session.

use super::*;

pub(super) struct CancelSafeRequestCleanup {
    id: String,
    requests: Arc<Mutex<HashMap<String, PendingRequest>>>,
    state: Arc<Mutex<SessionState>>,
    wake: mpsc::Sender<()>,
    armed: bool,
}

impl CancelSafeRequestCleanup {
    pub(super) fn new(session: &RelaySession, id: String) -> Self {
        Self {
            id,
            requests: Arc::clone(&session.requests),
            state: Arc::clone(&session.state),
            wake: session.wake.clone(),
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelSafeRequestCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let id = self.id.clone();
        let requests = Arc::clone(&self.requests);
        let state = Arc::clone(&self.state);
        let wake = self.wake.clone();
        tokio::spawn(async move {
            requests.lock().await.remove(&id);
            let mut state = state.lock().await;
            state.transient.retain(|subscription| subscription.id != id);
            state.removed.insert(id);
            drop(state);
            let _ = wake.try_send(());
        });
    }
}

/// State-bearing delivery for the one active Huddle authority watcher.
pub(crate) enum HuddleRelaySignal {
    Ready,
    Event(Box<Event>),
    Unavailable,
}

pub(super) async fn is_current_huddle_subscription(
    session: &RelaySession,
    subscription_id: &str,
) -> bool {
    session
        .state
        .lock()
        .await
        .huddle
        .as_ref()
        .is_some_and(|subscription| subscription.id == subscription_id)
}

pub(super) async fn route_huddle_event(
    session: &RelaySession,
    subscription_id: &str,
    event: Box<Event>,
) -> Option<Box<Event>> {
    if is_current_huddle_subscription(session, subscription_id).await {
        notify_huddle(session, HuddleRelaySignal::Event(event)).await;
        return None;
    }
    (!subscription_id.starts_with("huddle:")).then_some(event)
}

impl NativeRelayClient {
    /// Attach Huddle monitoring only to the compatible native scope. A
    /// mismatched archive scope is never displaced and never causes a second
    /// persistent Huddle socket.
    pub(crate) async fn huddle_session(
        &self,
        relay_url: String,
        keys: Keys,
        media_fence: CancellationToken,
    ) -> Result<(Arc<RelaySession>, mpsc::Receiver<HuddleRelaySignal>), String> {
        let scope = (relay_url.clone(), keys.public_key().to_hex());
        let mut current = self.current.lock().await;
        let session = match current.as_ref() {
            Some(managed) if managed.scope == scope => Arc::clone(&managed.session),
            Some(_) => {
                return Err("native relay scope does not match the active Huddle".to_string())
            }
            None => {
                let session = start_managed(relay_url, keys, None);
                *current = Some(ManagedSession {
                    scope,
                    session: Arc::clone(&session),
                });
                session
            }
        };
        drop(current);
        let event_rx = session.attach_huddle(media_fence).await;
        Ok((session, event_rx))
    }
}

fn same_subscription(left: &Subscription, right: &Subscription) -> bool {
    left.id == right.id && left.filter == right.filter
}

impl RelaySession {
    /// Attach the sole active Huddle watcher to this shared relay session.
    pub(crate) async fn attach_huddle(
        &self,
        media_fence: CancellationToken,
    ) -> mpsc::Receiver<HuddleRelaySignal> {
        let (events, receiver) = mpsc::channel(64);
        *self.huddle_events.lock().await = Some(events);
        let mut fence = self.huddle_media_fence.lock().await;
        if let Some(previous) = fence.replace(media_fence) {
            previous.cancel();
        }
        receiver
    }

    /// Replace or remove the Huddle watcher without disturbing archive traffic.
    pub(crate) async fn set_huddle_subscription(&self, subscription: Option<Subscription>) {
        let mut state = self.state.lock().await;
        if let Some(previous) = std::mem::replace(&mut state.huddle, subscription) {
            let unchanged = state
                .huddle
                .as_ref()
                .is_some_and(|current| same_subscription(current, &previous));
            if !unchanged {
                state.removed.insert(previous.id);
            }
        }
        let removed = state.huddle.is_none();
        drop(state);
        if removed {
            self.huddle_media_fence.lock().await.take();
        }
        let _ = self.wake.try_send(());
    }

    /// Remove only the watcher owned by `subscription_id`. A stale runtime
    /// cannot tear down a replacement subscription.
    pub(crate) async fn clear_huddle_subscription(&self, subscription_id: &str) {
        {
            let mut state = self.state.lock().await;
            let owns_current = state
                .huddle
                .as_ref()
                .is_some_and(|current| current.id == subscription_id);
            if !owns_current {
                return;
            }
            let Some(previous) = state.huddle.take() else {
                return;
            };
            state.removed.insert(previous.id);
        }
        self.huddle_media_fence.lock().await.take();
        self.huddle_events.lock().await.take();
        let _ = self.wake.try_send(());
    }
}

pub(super) async fn notify_huddle(session: &RelaySession, signal: HuddleRelaySignal) {
    let closes_media = matches!(
        &signal,
        HuddleRelaySignal::Event(_) | HuddleRelaySignal::Unavailable
    );
    if closes_media {
        if let Some(fence) = session.huddle_media_fence.lock().await.as_ref() {
            fence.cancel();
        }
    }
    let sender = session.huddle_events.lock().await.clone();
    if let Some(sender) = sender {
        let _ = sender.send(signal).await;
    }
}
