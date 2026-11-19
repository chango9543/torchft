// Copyright (c) 2026 Huawei Technologies Co., Ltd.
// This source code is licensed under the BSD-style license found in LICENSE.

use super::*;
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

type Layout = BTreeMap<String, BTreeMap<String, Vec<u32>>>;
type Instances = BTreeMap<String, String>;

pub(super) struct Preemption {
    layout: Layout,
    token: String,
    lease: Option<Lease>,
    used_requests: HashSet<String>,
    retired: HashSet<String>,
}

#[derive(Clone, Copy, PartialEq, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Preparing,
    Draining,
    Released,
    Joining,
    Restored,
    Cancelled,
}

#[derive(Serialize)]
struct Lease {
    request: String,
    cards: usize,
    node: Option<String>,
    phase: Phase,
    targets: Instances,
    incumbents: Instances,
    manager_exits: BTreeSet<String>,
    receipts: BTreeSet<(String, String)>,
    #[serde(skip)]
    since: Instant,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum Command {
    Status,
    Suspend { request: String, cards: usize, node: Option<String> },
    ManagerExited { request: String, slot: String, instance: String },
    Released { request: String, slot: String, instance: String, node: String },
    Resume { request: String },
}

// Manager appends a fresh UUID to its configured replica name at every launch.
fn slot(instance: &str) -> &str {
    instance.rsplit_once(':').map_or(instance, |(name, _)| name)
}

impl Preemption {
    pub(super) fn from_env() -> Result<Option<Self>> {
        let Some(path) = std::env::var_os("TORCHFT_PREEMPT_LAYOUT") else {
            return Ok(None);
        };
        let layout: Layout = serde_json::from_slice(&std::fs::read(path)?)?;
        let token = std::env::var("TORCHFT_PREEMPT_TOKEN")?;
        anyhow::ensure!(!token.is_empty() && !layout.is_empty(), "empty token or layout");
        let mut devices = BTreeSet::new();
        for (name, nodes) in &layout {
            anyhow::ensure!(!name.is_empty() && !nodes.is_empty(), "empty replica placement");
            for (node, cards) in nodes {
                anyhow::ensure!(!node.is_empty() && !cards.is_empty(), "empty node placement");
                for card in cards {
                    anyhow::ensure!(devices.insert((node, card)), "device assigned twice");
                }
            }
        }
        Ok(Some(Self {
            layout, token, lease: None,
            retired: HashSet::new(),
            used_requests: HashSet::new(),
        }))
    }

    pub(super) fn excluded(&self, id: &str) -> bool {
        self.retired.contains(id) || self.lease.as_ref().is_some_and(|lease| {
            matches!(lease.phase, Phase::Released | Phase::Draining)
                && lease.targets.contains_key(slot(id))
        })
    }

    fn suspend(&mut self, request: String, cards: usize, node: Option<String>, quorum: Option<&Quorum>, minimum: usize) -> Result<()> {
        anyhow::ensure!(!request.is_empty() && cards > 0, "specify a request and positive card count");
        if let Some(lease) = &self.lease {
            if lease.request == request {
                anyhow::ensure!(lease.cards == cards && lease.node == node, "request parameters changed");
                return Ok(());
            }
            anyhow::ensure!(matches!(lease.phase, Phase::Restored | Phase::Cancelled), "another membership change is active");
        }
        let quorum = quorum.ok_or_else(|| anyhow!("no published quorum"))?;
        let incumbents: Instances = quorum.participants.iter()
            .map(|m| (slot(&m.replica_id).to_owned(), m.replica_id.clone())).collect();
        anyhow::ensure!(incumbents.len() == quorum.participants.len() && incumbents.keys().eq(self.layout.keys()),
            "wait for configured replicas to join");

        // Count only cards in the requested location. Releasing the largest
        // contributors first minimizes the number of whole replicas stopped.
        let mut choices = Vec::new();
        for (name, nodes) in &self.layout {
            let count: usize = nodes.iter()
                .filter(|(n, _)| node.as_ref().is_none_or(|wanted| wanted == *n))
                .map(|(_, devices)| devices.len()).sum();
            if count > 0 {
                choices.push((count, name));
            }
        }
        choices.sort_by(|a, b| b.cmp(a));
        let mut count = 0;
        let mut targets = Instances::new();
        for (n, name) in choices.into_iter().take(incumbents.len().saturating_sub(minimum.max(1))) {
            targets.insert(name.clone(), incumbents[name].clone());
            count += n;
            if count >= cards {
                break;
            }
        }
        anyhow::ensure!(count >= cards, "insufficient releasable cards");
        anyhow::ensure!(self.used_requests.insert(request.clone()), "request ID has already completed");
        self.lease = Some(Lease {
            request, cards, node, targets, incumbents, phase: Phase::Preparing,
            manager_exits: BTreeSet::new(), receipts: BTreeSet::new(), since: Instant::now(),
        });
        Ok(())
    }

    fn command(&mut self, cmd: Command, quorum: Option<&Quorum>, minimum: usize) -> Result<()> {
        match cmd {
            Command::Status => (),
            Command::Suspend { request, cards, node } => return self.suspend(request, cards, node, quorum, minimum),
            Command::ManagerExited { request, slot, instance } => {
                self.target(&request, &slot, &instance)?.manager_exits.insert(slot);
            }
            Command::Released { request, slot, instance, node } => {
                anyhow::ensure!(self.layout.get(&slot).is_some_and(|nodes| nodes.contains_key(&node)), "unknown node");
                let lease = self.target(&request, &slot, &instance)?;
                anyhow::ensure!(lease.manager_exits.contains(&slot), "native Manager exit not confirmed");
                lease.receipts.insert((slot, node));
                let lease = self.lease.as_mut().unwrap();
                let all_released = lease.targets.keys().all(|name| self.layout[name].keys().all(|n|
                    lease.receipts.contains(&(name.clone(), n.clone()))));
                if all_released {
                    lease.phase = Phase::Released;
                }
            }
            Command::Resume { request } => {
                let lease = self.lease.as_mut().ok_or_else(|| anyhow!("no lease"))?;
                anyhow::ensure!(lease.request == request && matches!(lease.phase, Phase::Released | Phase::Joining | Phase::Restored),
                    "request is not released");
                if lease.phase == Phase::Released {
                    lease.phase = Phase::Joining;
                }
            }
        }
        Ok(())
    }

    fn target(&mut self, request: &str, name: &str, id: &str) -> Result<&mut Lease> {
        let lease = self.lease.as_mut().ok_or_else(|| anyhow!("no lease"))?;
        anyhow::ensure!(lease.request == request && lease.targets.get(name).is_some_and(|v| v == id)
            && matches!(lease.phase, Phase::Draining | Phase::Released), "stale release acknowledgement");
        Ok(lease)
    }

    // New RPCs and periodic ticks take the same lock. Once all incumbents are
    // waiting here, no old member can start training with a stale quorum.
    pub(super) fn before_tick(&mut self, participants: &mut HashMap<String, QuorumMemberDetails>,
        heartbeats: &mut HashMap<String, Instant>, heartbeat_timeout: Duration, previous: Option<&Quorum>) -> bool {
        let Some(lease) = self.lease.as_mut() else { return true };
        match lease.phase {
            Phase::Preparing => {
                if lease.since.elapsed() > Duration::from_secs(30) {
                    lease.phase = Phase::Cancelled;
                    return true;
                }
                if !lease.incumbents.values().all(|id| participants.contains_key(id)
                    && heartbeats.get(id).is_some_and(|last| last.elapsed() < heartbeat_timeout)) {
                    return false;
                }
                if participants.len() != lease.incumbents.len() {
                    lease.phase = Phase::Cancelled;
                    return true;
                }
                lease.phase = Phase::Draining;
                for id in lease.targets.values() {
                    let member = participants.remove(id).unwrap().member;
                    heartbeats.remove(id);
                    self.retired.insert(id.clone());
                    // Capture the address before replacing prev_quorum. Network I/O
                    // runs outside the state lock; only launcher receipts release cards.
                    tokio::spawn(kill_instance(member));
                }
            }
            Phase::Joining => {
                // A returning replica can finish startup much later than its peers.
                // Keep survivors training until a returning Manager actually arrives.
                if participants.keys().any(|id| lease.targets.contains_key(slot(id))) {
                    return previous.is_some_and(|q| q.participants.iter().all(|m|
                        participants.contains_key(&m.replica_id)));
                }
            }
            _ => (),
        }
        true
    }

    pub(super) fn published(&mut self, quorum: &Quorum) {
        if let Some(lease) = &mut self.lease {
            if lease.phase == Phase::Joining && lease.incumbents.keys().all(|name|
                quorum.participants.iter().any(|m| slot(&m.replica_id) == name)) {
                lease.phase = Phase::Restored;
            }
        }
    }
}

async fn kill_instance(member: QuorumMember) {
    let result = async {
        let mut client = manager_client_new(member.address, Duration::from_secs(10)).await?;
        client.kill(Request::new(KillRequest { msg: "suspended by Lighthouse".into() })).await?;
        Ok::<_, anyhow::Error>(())
    }.await;
    // Kill exits inside the RPC, so even successful delivery can return EOF.
    info!("planned Kill {}: {:?}; waiting for node acknowledgement", member.replica_id, result);
}

pub(super) async fn control(lh: Arc<Lighthouse>, headers: HeaderMap, Json(cmd): Json<Command>) -> impl IntoResponse {
    let mut state = lh.state.lock().await;
    let State { preemption, prev_quorum, .. } = &mut *state;
    let Some(p) = preemption else {
        return (StatusCode::NOT_FOUND, "preemption disabled".to_owned());
    };
    if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(&format!("Bearer {}", p.token)) {
        return (StatusCode::UNAUTHORIZED, "invalid token".to_owned());
    }
    if let Err(error) = p.command(cmd, prev_quorum.as_ref(), lh.opt.min_replicas as usize) {
        return (StatusCode::CONFLICT, error.to_string());
    }
    let members = prev_quorum.as_ref().map(|q| q.participants.iter()
        .map(|m| (&m.replica_id, m.step)).collect::<BTreeMap<_, _>>());
    (StatusCode::OK, serde_json::json!({"lease":p.lease,"layout":p.layout,"quorum":members}).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn suspend(request: &str, cards: usize) -> Command {
        Command::Suspend { request: request.into(), cards, node: Some("west".into()) }
    }

    #[tokio::test]
    async fn planned_shrink_waits_for_the_round_and_all_node_receipts() -> Result<()> {
        let layout = serde_json::from_str(r#"{"a":{"east":[2]},"b":{"west":[0,1],"east":[0,1]},"c":{"west":[2]}}"#)?;
        let mut p = Preemption {layout, token:"test".into(), lease:None, retired:Default::default(), used_requests:Default::default()};
        let members: Vec<_> = ["a:one","b:one","c:one"].iter().map(|id|QuorumMember{
            replica_id:id.to_string(), address:"http://127.0.0.1:1".into(), ..Default::default()
        }).collect();
        let q = Quorum{participants:members.clone(), ..Default::default()};
        assert!(p.command(suspend("too-many",3),Some(&q),2).is_err());
        p.command(suspend("job",2),Some(&q),2)?;
        let mut participants = HashMap::new();
        let mut beats = HashMap::new();
        let now=Instant::now();
        for (i, mut member) in members.into_iter().enumerate() {
            member.step=i as i64;
            beats.insert(member.replica_id.clone(),now);
            participants.insert(member.replica_id.clone(),QuorumMemberDetails{member,joined:now});
            assert_eq!(p.before_tick(&mut participants,&mut beats,Duration::from_secs(5),Some(&q)),i==2);
        }
        assert!(p.excluded("b:one"));
        assert_eq!(participants.len(),2);
        assert!(!beats.contains_key("b:one"));
        let resume=||Command::Resume{request:"job".into()};
        assert!(p.command(resume(),Some(&q),2).is_err());
        p.command(Command::ManagerExited{request:"job".into(),slot:"b".into(),instance:"b:one".into()},Some(&q),2)?;
        for (node,phase) in [("west",Phase::Draining),("east",Phase::Released)] {
            p.command(Command::Released{request:"job".into(),slot:"b".into(),instance:"b:one".into(),node:node.into()},Some(&q),2)?;
            assert_eq!(p.lease.as_ref().unwrap().phase,phase);
        }
        p.command(resume(),Some(&q),2)?;
        assert!(p.excluded("b:one"));
        assert!(!p.excluded("b:two"));
        Ok(())
    }
}
