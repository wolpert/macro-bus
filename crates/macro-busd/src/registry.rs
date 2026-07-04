//! The shared in-memory registry: message-type ownership (auth keys),
//! per-type subscriber sets, message-id generation, fan-out with the tail-drop
//! slow-consumer policy, and the loop-prevention seen-set.
//!
//! Everything here is in RAM and rebuilt as clients/peers (re)connect. A daemon
//! restart is a clean slate, by design.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use macro_bus_proto::status::{self, Code};
use macro_bus_proto::Message;
use tokio::sync::mpsc;
use tokio::sync::Notify;

/// One item queued toward a single connection's writer.
#[derive(Debug)]
pub enum Outbound {
    /// Deliver a message (`101 MSG` + body) to the subscriber.
    Deliver(Arc<Message>),
    /// An informational `190 NOTE` line (text after the code).
    Note(String),
}

/// A cheap, cloneable handle to a connection's outbound path, stored in the
/// registry's subscriber sets. All subscriptions of one connection share the
/// same handle (same queue, same drop bookkeeping).
#[derive(Clone)]
pub struct ConnHandle {
    /// Unique connection id.
    pub id: u64,
    /// Bounded outbound queue toward this connection's writer task.
    pub tx: mpsc::Sender<Outbound>,
    /// Per-type count of messages tail-dropped since the last `102 DROP`.
    pub drops: Arc<Mutex<HashMap<String, u64>>>,
    /// Woken whenever `drops` gains an entry so the writer emits `102 DROP`.
    pub drop_notify: Arc<Notify>,
}

impl ConnHandle {
    /// Record `n` tail-drops for `type_name` and wake the writer.
    fn record_drop(&self, type_name: &str) {
        {
            let mut d = self.drops.lock().unwrap();
            *d.entry(type_name.to_string()).or_insert(0) += 1;
        }
        self.drop_notify.notify_one();
    }
}

/// Ownership record for a registered message type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeReg {
    /// The authorization key required to publish.
    pub key: String,
    /// The daemon on which the owning registration originated.
    pub origin_daemon: String,
    /// Registration timestamp (ms since the Unix epoch), for tie-breaking.
    pub ts: u64,
}

impl TypeReg {
    /// The `(timestamp, daemon-id)` tuple used to resolve concurrent
    /// registrations: the numerically/lexicographically **lowest** tuple wins.
    fn tuple(&self) -> (u64, &str) {
        (self.ts, &self.origin_daemon)
    }
}

/// Error registering a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterError {
    /// MBP status code (`433`).
    pub code: Code,
    /// Human-readable reason.
    pub reason: String,
}

/// Error publishing a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishError {
    /// MBP status code (`430` / `441` / `452`).
    pub code: Code,
    /// Human-readable reason.
    pub reason: String,
}

/// Bounded FIFO set of recently-seen message ids for loop prevention.
struct SeenSet {
    cap: usize,
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl SeenSet {
    fn new(cap: usize) -> Self {
        SeenSet { cap: cap.max(1), set: HashSet::new(), order: VecDeque::new() }
    }

    /// Insert `id`. Returns `true` if it was newly inserted (i.e. NOT seen
    /// before), `false` if it was already present.
    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        self.set.insert(id.to_string());
        self.order.push_back(id.to_string());
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

struct Inner {
    /// type -> ownership record.
    types: HashMap<String, TypeReg>,
    /// type -> (conn id -> handle).
    subs: HashMap<String, HashMap<u64, ConnHandle>>,
}

/// The shared registry. Cloneable-by-`Arc`; all methods take `&self`.
pub struct Registry {
    daemon_id: String,
    seq: AtomicU64,
    inner: Mutex<Inner>,
    seen: Mutex<SeenSet>,
}

/// Outcome of a local `REGISTER`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registered {
    /// The full ownership record now held for the type.
    pub reg: TypeReg,
    /// True if this call created or changed the record (worth propagating),
    /// false if it was an idempotent no-op.
    pub changed: bool,
}

impl Registry {
    /// Create a registry for `daemon_id` with a seen-set of `seen_capacity`.
    pub fn new(daemon_id: impl Into<String>, seen_capacity: usize) -> Self {
        Registry {
            daemon_id: daemon_id.into(),
            seq: AtomicU64::new(1),
            inner: Mutex::new(Inner { types: HashMap::new(), subs: HashMap::new() }),
            seen: Mutex::new(SeenSet::new(seen_capacity)),
        }
    }

    /// This daemon's id.
    pub fn daemon_id(&self) -> &str {
        &self.daemon_id
    }

    /// Generate a fresh, cluster-unique message id: `<daemon-id>-<hex-seq>`.
    pub fn next_msg_id(&self) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        format!("{}-{:x}", self.daemon_id, n)
    }

    /// Handle a local `REGISTER <type> <key>` at wall-clock time `now_ms`.
    ///
    /// First-registrant wins: an unknown type is claimed; re-registering with
    /// the SAME key is idempotent; a DIFFERENT key is rejected with `433`.
    pub fn register_local(
        &self,
        type_name: &str,
        key: &str,
        now_ms: u64,
    ) -> Result<Registered, RegisterError> {
        let mut inner = self.inner.lock().unwrap();
        match inner.types.get(type_name) {
            None => {
                let reg = TypeReg {
                    key: key.to_string(),
                    origin_daemon: self.daemon_id.clone(),
                    ts: now_ms,
                };
                inner.types.insert(type_name.to_string(), reg.clone());
                Ok(Registered { reg, changed: true })
            }
            Some(existing) if existing.key == key => {
                Ok(Registered { reg: existing.clone(), changed: false })
            }
            Some(_) => Err(RegisterError {
                code: status::ALREADY_REGISTERED,
                reason: format!("{type_name} already registered"),
            }),
        }
    }

    /// Apply a registration learned from a peer, resolving conflicts by the
    /// deterministic lowest-`(ts, daemon-id)` rule.
    ///
    /// Returns `Some(reg)` with the now-effective record if the local table
    /// changed (caller should re-propagate), or `None` if the incoming record
    /// lost / was a duplicate and nothing changed.
    pub fn apply_remote_registration(
        &self,
        type_name: &str,
        incoming: TypeReg,
    ) -> Option<TypeReg> {
        let mut inner = self.inner.lock().unwrap();
        match inner.types.get(type_name) {
            None => {
                inner.types.insert(type_name.to_string(), incoming.clone());
                Some(incoming)
            }
            Some(existing) => {
                // Same record (idempotent) => no change.
                if existing.key == incoming.key
                    && existing.origin_daemon == incoming.origin_daemon
                    && existing.ts == incoming.ts
                {
                    return None;
                }
                // Deterministic winner: lowest (ts, daemon-id) tuple.
                if incoming.tuple() < existing.tuple() {
                    inner.types.insert(type_name.to_string(), incoming.clone());
                    Some(incoming)
                } else {
                    None
                }
            }
        }
    }

    /// Snapshot the full type table (for reconciliation with a newly-connected
    /// peer).
    pub fn all_registrations(&self) -> Vec<(String, TypeReg)> {
        let inner = self.inner.lock().unwrap();
        inner.types.iter().map(|(t, r)| (t.clone(), r.clone())).collect()
    }

    /// List known type names (sorted). Keys are never disclosed.
    pub fn list_types(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        let mut v: Vec<String> = inner.types.keys().cloned().collect();
        v.sort();
        v
    }

    /// Add `handle` as a subscriber of `type_name`. Idempotent per connection.
    pub fn subscribe(&self, type_name: &str, handle: ConnHandle) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .subs
            .entry(type_name.to_string())
            .or_default()
            .insert(handle.id, handle);
    }

    /// Remove connection `conn_id` from `type_name`.
    pub fn unsubscribe(&self, type_name: &str, conn_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(set) = inner.subs.get_mut(type_name) {
            set.remove(&conn_id);
            if set.is_empty() {
                inner.subs.remove(type_name);
            }
        }
    }

    /// Remove connection `conn_id` from every type it subscribed to.
    pub fn remove_conn(&self, conn_id: u64, types: &HashSet<String>) {
        let mut inner = self.inner.lock().unwrap();
        for t in types {
            if let Some(set) = inner.subs.get_mut(t) {
                set.remove(&conn_id);
                if set.is_empty() {
                    inner.subs.remove(t);
                }
            }
        }
    }

    /// Check whether a `PUBLISH <type> <key>` would be authorized, WITHOUT
    /// publishing. Used to reject before inviting the DATA body. Returns
    /// `Some(err)` if it would fail, `None` if it would be accepted.
    pub fn publish_precheck(&self, type_name: &str, key: &str) -> Option<PublishError> {
        let inner = self.inner.lock().unwrap();
        match inner.types.get(type_name) {
            None => Some(PublishError {
                code: status::UNKNOWN_TYPE,
                reason: format!("{type_name} not registered"),
            }),
            Some(reg) if reg.key != key => Some(PublishError {
                code: status::KEY_MISMATCH,
                reason: "authorization key mismatch".to_string(),
            }),
            Some(_) => None,
        }
    }

    /// Validate a local `PUBLISH <type> <key>` and, on success, build the
    /// [`Message`] (assigning a fresh id + this daemon as origin), record it in
    /// the seen-set, and fan it out to local subscribers.
    ///
    /// Returns the message so the caller can forward it to peers. Fan-out uses
    /// the tail-drop policy and never blocks on a slow subscriber.
    pub fn publish_local(
        &self,
        type_name: &str,
        key: &str,
        body: Vec<String>,
    ) -> Result<Arc<Message>, PublishError> {
        // Authorize under the registry lock, then fan out while still holding
        // it (fan-out is non-blocking try_send, so this is safe and keeps the
        // subscriber set stable during delivery).
        let inner = self.inner.lock().unwrap();
        match inner.types.get(type_name) {
            None => {
                return Err(PublishError {
                    code: status::UNKNOWN_TYPE,
                    reason: format!("{type_name} not registered"),
                })
            }
            Some(reg) if reg.key != key => {
                return Err(PublishError {
                    code: status::KEY_MISMATCH,
                    reason: "authorization key mismatch".to_string(),
                })
            }
            Some(_) => {}
        }

        let msg = Arc::new(Message::new(
            type_name,
            self.next_msg_id(),
            self.daemon_id.clone(),
            body,
        ));
        // A locally-generated id is unique, so this always inserts.
        self.seen.lock().unwrap().insert(&msg.msg_id);
        self.fanout_locked(&inner, &msg);
        Ok(msg)
    }

    /// Ingest a message received from a peer. Returns `true` if it was new
    /// (delivered to local subscribers; caller should re-forward to other
    /// peers) or `false` if it was a duplicate and dropped.
    pub fn ingest_remote(&self, msg: Arc<Message>) -> bool {
        if !self.seen.lock().unwrap().insert(&msg.msg_id) {
            return false; // already seen -> loop prevention
        }
        let inner = self.inner.lock().unwrap();
        self.fanout_locked(&inner, &msg);
        true
    }

    /// Fan `msg` out to all current subscribers of its type. Caller holds the
    /// registry lock. Tail-drops on any full outbound queue.
    fn fanout_locked(&self, inner: &Inner, msg: &Arc<Message>) {
        let Some(subs) = inner.subs.get(&msg.type_name) else {
            return; // no listeners -> the message evaporates
        };
        for handle in subs.values() {
            match handle.tx.try_send(Outbound::Deliver(msg.clone())) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => handle.record_drop(&msg.type_name),
                Err(mpsc::error::TrySendError::Closed(_)) => { /* conn gone; cleaned up on drop */ }
            }
        }
    }

    /// Number of subscribers currently listening to `type_name` (for tests).
    #[cfg(test)]
    pub fn subscriber_count(&self, type_name: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.subs.get(type_name).map(|s| s.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(id: u64, depth: usize) -> (ConnHandle, mpsc::Receiver<Outbound>) {
        let (tx, rx) = mpsc::channel(depth);
        let h = ConnHandle {
            id,
            tx,
            drops: Arc::new(Mutex::new(HashMap::new())),
            drop_notify: Arc::new(Notify::new()),
        };
        (h, rx)
    }

    #[test]
    fn register_first_wins_same_key_idempotent() {
        let r = Registry::new("d1", 1024);
        let a = r.register_local("t", "k", 100).unwrap();
        assert!(a.changed);
        let b = r.register_local("t", "k", 200).unwrap();
        assert!(!b.changed, "same key is idempotent");
        let e = r.register_local("t", "other", 300).unwrap_err();
        assert_eq!(e.code, status::ALREADY_REGISTERED);
    }

    #[test]
    fn publish_requires_registration_and_key() {
        let r = Registry::new("d1", 1024);
        let e = r.publish_local("t", "k", vec!["x".into()]).unwrap_err();
        assert_eq!(e.code, status::UNKNOWN_TYPE);
        r.register_local("t", "k", 1).unwrap();
        let e = r.publish_local("t", "wrong", vec!["x".into()]).unwrap_err();
        assert_eq!(e.code, status::KEY_MISMATCH);
        assert!(r.publish_local("t", "k", vec!["x".into()]).is_ok());
    }

    #[test]
    fn fanout_delivers_to_subscribers() {
        let r = Registry::new("d1", 1024);
        r.register_local("t", "k", 1).unwrap();
        let (h1, mut rx1) = handle(1, 8);
        let (h2, mut rx2) = handle(2, 8);
        r.subscribe("t", h1);
        r.subscribe("t", h2);
        assert_eq!(r.subscriber_count("t"), 2);
        let msg = r.publish_local("t", "k", vec!["hi".into()]).unwrap();
        for rx in [&mut rx1, &mut rx2] {
            match rx.try_recv().unwrap() {
                Outbound::Deliver(m) => {
                    assert_eq!(m.body, vec!["hi".to_string()]);
                    assert_eq!(m.msg_id, msg.msg_id);
                    assert_eq!(m.origin, "d1");
                }
                _ => panic!("expected deliver"),
            }
        }
    }

    #[test]
    fn tail_drop_records_and_notifies() {
        let r = Registry::new("d1", 1024);
        r.register_local("t", "k", 1).unwrap();
        let (h, _rx) = handle(1, 1); // depth 1, we never drain
        let drops = h.drops.clone();
        r.subscribe("t", h);
        // First publish fills the queue; subsequent ones tail-drop.
        r.publish_local("t", "k", vec!["1".into()]).unwrap();
        r.publish_local("t", "k", vec!["2".into()]).unwrap();
        r.publish_local("t", "k", vec!["3".into()]).unwrap();
        let d = drops.lock().unwrap();
        assert_eq!(*d.get("t").unwrap(), 2, "two messages tail-dropped");
    }

    #[test]
    fn ingest_remote_dedup() {
        let r = Registry::new("d2", 1024);
        r.register_local("t", "k", 1).unwrap();
        let (h, mut rx) = handle(1, 8);
        r.subscribe("t", h);
        let m = Arc::new(Message::new("t", "d1-1", "d1", vec!["hi".into()]));
        assert!(r.ingest_remote(m.clone()), "first is new");
        assert!(!r.ingest_remote(m.clone()), "second is a dup -> dropped");
        // Only one delivery happened.
        assert!(matches!(rx.try_recv(), Ok(Outbound::Deliver(_))));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn remote_registration_conflict_lowest_tuple_wins() {
        let r = Registry::new("d2", 1024);
        // Local registers t with a late timestamp.
        r.register_local("t", "localkey", 500).unwrap();
        // Peer d1 registered earlier -> wins.
        let winner = r
            .apply_remote_registration(
                "t",
                TypeReg { key: "peerkey".into(), origin_daemon: "d1".into(), ts: 100 },
            )
            .expect("incoming wins");
        assert_eq!(winner.key, "peerkey");
        // A later (higher-ts) incoming loses.
        let none = r.apply_remote_registration(
            "t",
            TypeReg { key: "z".into(), origin_daemon: "d0".into(), ts: 999 },
        );
        assert!(none.is_none());
    }

    #[test]
    fn seen_set_evicts_oldest() {
        let mut s = SeenSet::new(2);
        assert!(s.insert("a"));
        assert!(s.insert("b"));
        assert!(!s.insert("a")); // still present
        assert!(s.insert("c")); // evicts "a"
        assert!(s.insert("a")); // "a" was evicted -> treated as new again
    }
}
