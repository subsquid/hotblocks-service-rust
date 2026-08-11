//! Backend affinity behind one endpoint URL.

use std::sync::{Arc, Mutex};

/// An upstream-assigned name for one backend behind an endpoint, held opaquely
/// and replayed. An endpoint that names nothing leaves requests unbound.
///
/// Belongs to the request, not the client: two in-flight requests on one client
/// legitimately target different backends.
#[derive(Clone, Debug, Default)]
pub struct UpstreamSession(Arc<Mutex<Option<Arc<str>>>>);

impl UpstreamSession {
    /// Unpinned: whichever backend answers first claims the binding.
    pub fn new() -> Self {
        UpstreamSession::default()
    }

    /// The name to replay, once an answer has carried one.
    pub fn pinned(&self) -> Option<Arc<str>> {
        self.0.lock().expect("upstream session").clone()
    }

    /// Silence means the backend is still ours, so only a named assignment
    /// replaces one — which is what makes a fleet roll self-repairing.
    pub fn adopt(&self, assignment: Option<Arc<str>>) {
        if let Some(assignment) = assignment {
            *self.0.lock().expect("upstream session") = Some(assignment);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unnamed_answer_leaves_the_binding_standing() {
        let session = UpstreamSession::new();
        assert!(session.pinned().is_none());

        session.adopt(Some(Arc::from("id=1")));
        session.adopt(None);
        assert_eq!(session.pinned().as_deref(), Some("id=1"));

        session.adopt(Some(Arc::from("id=2")));
        assert_eq!(session.pinned().as_deref(), Some("id=2"));
    }
}
