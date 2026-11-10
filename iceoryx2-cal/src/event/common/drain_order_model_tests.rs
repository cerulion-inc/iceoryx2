// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Exhaustive interleaving model of one listener drain against two concurrent notifiers.
//!
//! `Handle::notify()` and `Waiter::drain_events()` are modeled step by step. Every access to
//! the notification state, the trigger or the event state is one atomic step, and the steps of
//! the three actors interleave in every possible order, sequentially consistent. The trigger is
//! a count of pending wake units, the event state one pending count per event id. One drain
//! runs at a time: the port listener serializes its drains behind a mutex.
//!
//! A notify is four steps: activate its event id; compare-exchange the state from idle to
//! pending, where a notified state ends the notify and a pending state sends anyway; send one
//! unit into the trigger; compare-exchange the state from pending to notified.
//!
//! Every quiescent end (each notifier done, the drain done or blocked in its wait) must be:
//!
//!  * not deaf: the state is not notified with an empty trigger. In that state every later
//!    notify skips its send, so the listener never wakes again.
//!  * without a lost wake: no activation is pending with an empty trigger while the state is
//!    not notified. Later notifies still wake the listener, but this activation has no wake.
//!
//! A timed wait behaves as a try wait when it times out on an empty trigger and as a blocking
//! wait otherwise, so the two wait kinds cover it.

use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

/// Event ids 0 to 2: id 0 is activated by the start state, the notifiers use ids 1 and 2.
const EVENT_IDS: usize = 3;
const PRIOR: usize = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NotificationState {
    Idle,
    Pending,
    Notified,
}

/// How many wake units one receive takes from the trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TriggerRead {
    /// A unix datagram socket: one datagram per receive.
    OneUnit,
    /// A socket pair stream, or a semaphore decremented until it is empty: every unit.
    EveryUnit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitKind {
    /// `try_wait()`: takes what the trigger holds, never blocks.
    Try,
    /// `blocking_wait()`: blocks until the trigger holds a unit, then takes.
    Blocking,
}

/// How a drain reads the notification state to choose its arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateCheck {
    /// `compare_exchange(NOTIFIED, IDLE)`: the notified arm runs when it succeeds.
    SwapNotifiedForIdle,
    /// `load() == NOTIFIED`: the notified arm runs when it is.
    LoadNotified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainStep {
    /// The trigger wait call of `try_wait()` or `blocking_wait()`.
    WaitCall,
    /// `empty_buffer()`: one receive.
    EmptyBuffer,
    /// `store(IDLE)`.
    Rearm,
    /// `compare_exchange(NOTIFIED, IDLE)`, result ignored.
    RearmIfNotified,
    /// Takes the pending count of every event id, one id per step.
    DrainEventState,
}

use DrainStep::{DrainEventState, EmptyBuffer, Rearm, RearmIfNotified, WaitCall};
use NotificationState::{Idle, Notified, Pending};

#[derive(Debug)]
struct DrainOrder {
    name: &'static str,
    check: StateCheck,
    notified_arm: &'static [DrainStep],
    other_arm: &'static [DrainStep],
}

/// The notified arm re-arms in its swap; the other arm waits and re-arms. Both then empty the
/// trigger and drain the event state, so the re-arm comes before the trigger is consumed.
const REARM_BEFORE_CONSUME: DrainOrder = DrainOrder {
    name: "rearm before consume",
    check: StateCheck::SwapNotifiedForIdle,
    notified_arm: &[EmptyBuffer, DrainEventState],
    other_arm: &[WaitCall, Rearm, EmptyBuffer, DrainEventState],
};

/// [`REARM_BEFORE_CONSUME`], then a swap of notified for idle after both drains.
const REARM_AFTER_DRAINS_IF_NOTIFIED: DrainOrder = DrainOrder {
    name: "rearm before consume, then swap notified for idle after both drains",
    check: StateCheck::SwapNotifiedForIdle,
    notified_arm: &[EmptyBuffer, DrainEventState, RearmIfNotified],
    other_arm: &[
        WaitCall,
        Rearm,
        EmptyBuffer,
        DrainEventState,
        RearmIfNotified,
    ],
};

/// [`REARM_BEFORE_CONSUME`], then an idle store after both drains.
const REARM_AFTER_DRAINS: DrainOrder = DrainOrder {
    name: "rearm before consume, then store idle after both drains",
    check: StateCheck::SwapNotifiedForIdle,
    notified_arm: &[EmptyBuffer, DrainEventState, Rearm],
    other_arm: &[WaitCall, Rearm, EmptyBuffer, DrainEventState, Rearm],
};

/// Loads the state; a listener that is not notified waits. Then the drain empties the
/// trigger, re-arms and drains the event state.
const CONSUME_BEFORE_REARM: DrainOrder = DrainOrder {
    name: "consume before rearm",
    check: StateCheck::LoadNotified,
    notified_arm: &[EmptyBuffer, Rearm, DrainEventState],
    other_arm: &[WaitCall, EmptyBuffer, Rearm, DrainEventState],
};

/// The order `Waiter::drain_events()` implements.
const DRAIN_EVENTS_ORDER: &DrainOrder = &REARM_BEFORE_CONSUME;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum DrainAt {
    Check,
    Arm {
        notified: bool,
        step: usize,
        event_id: usize,
    },
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NotifyAt {
    Activate,
    ClaimPending,
    Send,
    MarkNotified,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct World {
    state: NotificationState,
    /// Wake units in the trigger.
    units: u8,
    /// Activations not yet drained, per event id.
    pending: [u8; EVENT_IDS],
    /// Activations the drain delivered, per event id.
    delivered: [u8; EVENT_IDS],
    drain: DrainAt,
    notifiers: [NotifyAt; 2],
}

impl World {
    fn is_deaf(&self) -> bool {
        self.state == Notified && self.units == 0
    }

    fn loses_a_wake(&self) -> bool {
        self.state != Notified && self.units == 0 && self.pending.iter().any(|n| *n > 0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Start {
    IdleEmpty,
    /// What a drain leaves when two notifies both sent while pending.
    IdleStrayUnit,
    /// One completed notify of the prior id.
    NotifiedOneUnit,
    /// One completed notify of the prior id plus one more unit.
    NotifiedTwoUnits,
}

impl Start {
    fn world(self) -> World {
        let (state, units, prior) = match self {
            Start::IdleEmpty => (Idle, 0, 0),
            Start::IdleStrayUnit => (Idle, 1, 0),
            Start::NotifiedOneUnit => (Notified, 1, 1),
            Start::NotifiedTwoUnits => (Notified, 2, 1),
        };
        let mut pending = [0; EVENT_IDS];
        pending[PRIOR] = prior;
        World {
            state,
            units,
            pending,
            delivered: [0; EVENT_IDS],
            drain: DrainAt::Check,
            notifiers: [NotifyAt::Activate; 2],
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Setup {
    start: Start,
    trigger: TriggerRead,
    wait: WaitKind,
    /// The event id each notifier activates.
    ids: [usize; 2],
}

fn every_setup() -> Vec<Setup> {
    let mut setups = Vec::new();
    for start in [
        Start::IdleEmpty,
        Start::IdleStrayUnit,
        Start::NotifiedOneUnit,
        Start::NotifiedTwoUnits,
    ] {
        for trigger in [TriggerRead::OneUnit, TriggerRead::EveryUnit] {
            for wait in [WaitKind::Try, WaitKind::Blocking] {
                for ids in [[1, 2], [1, 1]] {
                    setups.push(Setup {
                        start,
                        trigger,
                        wait,
                        ids,
                    });
                }
            }
        }
    }
    setups
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Actor {
    Drain,
    Notifier(usize),
}

const D: Actor = Actor::Drain;
const N0: Actor = Actor::Notifier(0);
const N1: Actor = Actor::Notifier(1);

fn take_units(trigger: TriggerRead, units: u8) -> u8 {
    match trigger {
        TriggerRead::OneUnit => units.saturating_sub(1),
        TriggerRead::EveryUnit => 0,
    }
}

fn after_step(arm: &[DrainStep], notified: bool, step: usize) -> DrainAt {
    if step + 1 < arm.len() {
        DrainAt::Arm {
            notified,
            step: step + 1,
            event_id: 0,
        }
    } else {
        DrainAt::Done
    }
}

/// The drain's next step, or `None` when it is done or blocked in its wait.
fn drain_step(order: &DrainOrder, setup: Setup, world: World) -> Option<World> {
    let mut next = world;
    match world.drain {
        DrainAt::Done => return None,
        DrainAt::Check => {
            let notified = world.state == Notified;
            if notified && order.check == StateCheck::SwapNotifiedForIdle {
                next.state = Idle;
            }
            next.drain = DrainAt::Arm {
                notified,
                step: 0,
                event_id: 0,
            };
        }
        DrainAt::Arm {
            notified,
            step,
            event_id,
        } => {
            let arm = if notified {
                order.notified_arm
            } else {
                order.other_arm
            };
            next.drain = after_step(arm, notified, step);
            match arm[step] {
                WaitCall => {
                    if setup.wait == WaitKind::Blocking && world.units == 0 {
                        return None;
                    }
                    next.units = take_units(setup.trigger, world.units);
                }
                EmptyBuffer => next.units = take_units(setup.trigger, world.units),
                Rearm => next.state = Idle,
                RearmIfNotified => {
                    if world.state == Notified {
                        next.state = Idle;
                    }
                }
                DrainEventState => {
                    next.delivered[event_id] += world.pending[event_id];
                    next.pending[event_id] = 0;
                    if event_id + 1 < EVENT_IDS {
                        next.drain = DrainAt::Arm {
                            notified,
                            step,
                            event_id: event_id + 1,
                        };
                    }
                }
            }
        }
    }
    Some(next)
}

/// The notifier's next step, or `None` when it is done.
fn notify_step(setup: Setup, world: World, notifier: usize) -> Option<World> {
    let mut next = world;
    let id = setup.ids[notifier];
    next.notifiers[notifier] = match world.notifiers[notifier] {
        NotifyAt::Done => return None,
        NotifyAt::Activate => {
            next.pending[id] += 1;
            NotifyAt::ClaimPending
        }
        NotifyAt::ClaimPending => match world.state {
            Idle => {
                next.state = Pending;
                NotifyAt::Send
            }
            Pending => NotifyAt::Send,
            Notified => NotifyAt::Done,
        },
        NotifyAt::Send => {
            next.units += 1;
            NotifyAt::MarkNotified
        }
        NotifyAt::MarkNotified => {
            if world.state == Pending {
                next.state = Notified;
            }
            NotifyAt::Done
        }
    };
    Some(next)
}

fn step(order: &DrainOrder, setup: Setup, world: World, actor: Actor) -> Option<World> {
    match actor {
        Actor::Drain => drain_step(order, setup, world),
        Actor::Notifier(n) => notify_step(setup, world, n),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Verdict {
    deaf: bool,
    lost_wake: bool,
}

/// A schedule that reaches a quiescent end, and that end.
type Witness = (Vec<Actor>, World);

#[derive(Default)]
struct Findings {
    deaf: Option<Witness>,
    lost_wake: Option<Witness>,
    ends: BTreeSet<World>,
    worlds_seen: usize,
}

impl Findings {
    fn verdict(&self) -> Verdict {
        Verdict {
            deaf: self.deaf.is_some(),
            lost_wake: self.lost_wake.is_some(),
        }
    }
}

fn visit(
    order: &DrainOrder,
    setup: Setup,
    world: World,
    seen: &mut BTreeSet<World>,
    schedule: &mut Vec<Actor>,
    findings: &mut Findings,
) {
    if !seen.insert(world) {
        return;
    }
    let mut quiescent = true;
    for actor in [D, N0, N1] {
        if let Some(next) = step(order, setup, world, actor) {
            quiescent = false;
            schedule.push(actor);
            visit(order, setup, next, seen, schedule, findings);
            schedule.pop();
        }
    }
    if quiescent {
        if world.is_deaf() && findings.deaf.is_none() {
            findings.deaf = Some((schedule.clone(), world));
        }
        if world.loses_a_wake() && findings.lost_wake.is_none() {
            findings.lost_wake = Some((schedule.clone(), world));
        }
        findings.ends.insert(world);
    }
}

/// Visits every interleaving of the drain and both notifiers from the setup's start.
fn explore(order: &DrainOrder, setup: Setup) -> Findings {
    let mut findings = Findings::default();
    let mut seen = BTreeSet::new();
    visit(
        order,
        setup,
        setup.start.world(),
        &mut seen,
        &mut Vec::new(),
        &mut findings,
    );
    findings.worlds_seen = seen.len();
    findings
}

/// Every property violation of `order` over every setup, one line each.
fn violations(order: &DrainOrder) -> Vec<String> {
    let mut lines = Vec::new();
    for setup in every_setup() {
        let findings = explore(order, setup);
        assert!(findings.worlds_seen > 0 && !findings.ends.is_empty());
        if let Some((schedule, end)) = &findings.deaf {
            lines.push(format!(
                "{}: deaf in {setup:?} after {schedule:?}, ending in {end:?}",
                order.name
            ));
        }
        if let Some((schedule, end)) = &findings.lost_wake {
            lines.push(format!(
                "{}: lost wake in {setup:?} after {schedule:?}, ending in {end:?}",
                order.name
            ));
        }
    }
    lines
}

fn verdict_over_every_setup(order: &DrainOrder) -> Verdict {
    let mut verdict = Verdict {
        deaf: false,
        lost_wake: false,
    };
    for setup in every_setup() {
        let found = explore(order, setup).verdict();
        verdict.deaf |= found.deaf;
        verdict.lost_wake |= found.lost_wake;
    }
    verdict
}

/// What the exhaustive search must find for each drain order. Every order but
/// [`CONSUME_BEFORE_REARM`] fails a property.
const BASELINE: [(&DrainOrder, Verdict); 4] = [
    (
        &REARM_BEFORE_CONSUME,
        Verdict {
            deaf: true,
            lost_wake: false,
        },
    ),
    (
        &REARM_AFTER_DRAINS_IF_NOTIFIED,
        Verdict {
            deaf: true,
            lost_wake: true,
        },
    ),
    (
        &REARM_AFTER_DRAINS,
        Verdict {
            deaf: false,
            lost_wake: true,
        },
    ),
    (
        &CONSUME_BEFORE_REARM,
        Verdict {
            deaf: false,
            lost_wake: false,
        },
    ),
];

fn replay(order: &DrainOrder, setup: Setup, schedule: &[Actor]) -> World {
    let mut world = setup.start.world();
    for (n, actor) in schedule.iter().enumerate() {
        world = step(order, setup, world, *actor)
            .unwrap_or_else(|| panic!("step {n} ({actor:?}) is not enabled in {world:?}"));
    }
    world
}

fn is_quiescent(order: &DrainOrder, setup: Setup, world: World) -> bool {
    [D, N0, N1]
        .iter()
        .all(|actor| step(order, setup, world, *actor).is_none())
}

const DATAGRAM_TRY_FROM_IDLE: Setup = Setup {
    start: Start::IdleEmpty,
    trigger: TriggerRead::OneUnit,
    wait: WaitKind::Try,
    ids: [1, 2],
};

#[test]
fn the_order_drain_events_implements_is_never_deaf_and_never_loses_a_wake() {
    let lines = violations(DRAIN_EVENTS_ORDER);
    assert!(
        lines.is_empty(),
        "{} violations:\n{}",
        lines.len(),
        lines.join("\n")
    );
}

#[test]
fn every_drain_order_matches_its_committed_verdict() {
    for (order, expected) in BASELINE {
        assert_eq!(
            verdict_over_every_setup(order),
            expected,
            "{}:\n{}",
            order.name,
            violations(order).join("\n")
        );
    }
}

#[test]
fn the_committed_baseline_fails_every_order_but_consume_before_rearm() {
    for (order, verdict) in BASELINE {
        let fails = verdict.deaf || verdict.lost_wake;
        assert_eq!(
            fails,
            order.name != CONSUME_BEFORE_REARM.name,
            "{}",
            order.name
        );
    }
}

#[test]
fn rearm_before_consume_goes_deaf_when_a_notify_sends_between_the_rearm_and_the_consume() {
    let schedule = [D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1];
    let end = replay(&REARM_BEFORE_CONSUME, DATAGRAM_TRY_FROM_IDLE, &schedule);

    assert_eq!(
        end,
        World {
            state: Notified,
            units: 0,
            pending: [0, 0, 1],
            delivered: [0, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(
        &REARM_BEFORE_CONSUME,
        DATAGRAM_TRY_FROM_IDLE,
        end
    ));
    assert!(end.is_deaf());
    assert!(
        explore(&REARM_BEFORE_CONSUME, DATAGRAM_TRY_FROM_IDLE)
            .ends
            .contains(&end)
    );
}

#[test]
fn consume_before_rearm_keeps_the_wake_of_a_notify_that_sends_during_the_drain() {
    let schedule = [D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1, N1, N1];
    let end = replay(&CONSUME_BEFORE_REARM, DATAGRAM_TRY_FROM_IDLE, &schedule);

    assert_eq!(
        end,
        World {
            state: Notified,
            units: 2,
            pending: [0, 0, 1],
            delivered: [0, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(
        &CONSUME_BEFORE_REARM,
        DATAGRAM_TRY_FROM_IDLE,
        end
    ));
    assert!(!end.is_deaf() && !end.loses_a_wake());
    assert!(
        explore(&CONSUME_BEFORE_REARM, DATAGRAM_TRY_FROM_IDLE)
            .ends
            .contains(&end)
    );
}

#[test]
fn rearm_after_drains_if_notified_goes_deaf_when_the_notifier_marks_after_the_swap() {
    let schedule = [D, D, D, N0, N0, N0, D, D, D, D, D, N0, N1, N1];
    let end = replay(
        &REARM_AFTER_DRAINS_IF_NOTIFIED,
        DATAGRAM_TRY_FROM_IDLE,
        &schedule,
    );

    assert_eq!(
        end,
        World {
            state: Notified,
            units: 0,
            pending: [0, 0, 1],
            delivered: [0, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(
        &REARM_AFTER_DRAINS_IF_NOTIFIED,
        DATAGRAM_TRY_FROM_IDLE,
        end
    ));
    assert!(end.is_deaf());
}

#[test]
fn rearm_after_drains_if_notified_loses_the_wake_of_a_notify_that_skipped_on_notified() {
    let schedule = [D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1, D];
    let end = replay(
        &REARM_AFTER_DRAINS_IF_NOTIFIED,
        DATAGRAM_TRY_FROM_IDLE,
        &schedule,
    );

    assert_eq!(
        end,
        World {
            state: Idle,
            units: 0,
            pending: [0, 0, 1],
            delivered: [0, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(
        &REARM_AFTER_DRAINS_IF_NOTIFIED,
        DATAGRAM_TRY_FROM_IDLE,
        end
    ));
    assert!(end.loses_a_wake());
}

#[test]
fn rearm_after_drains_loses_the_wake_of_a_notify_that_skipped_on_notified() {
    let schedule = [D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1, D];
    let end = replay(&REARM_AFTER_DRAINS, DATAGRAM_TRY_FROM_IDLE, &schedule);

    assert_eq!(
        end,
        World {
            state: Idle,
            units: 0,
            pending: [0, 0, 1],
            delivered: [0, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(
        &REARM_AFTER_DRAINS,
        DATAGRAM_TRY_FROM_IDLE,
        end
    ));
    assert!(end.loses_a_wake());
}

#[test]
fn a_blocking_drain_waits_for_a_unit_and_a_stream_read_takes_every_unit() {
    let setup = Setup {
        start: Start::IdleStrayUnit,
        trigger: TriggerRead::EveryUnit,
        wait: WaitKind::Blocking,
        ids: [1, 2],
    };
    let after_wait = replay(&CONSUME_BEFORE_REARM, setup, &[N0, N0, N0, D, D]);
    assert_eq!(after_wait.units, 0);
    assert_eq!(after_wait.state, Pending);

    let blocked = Setup {
        start: Start::IdleEmpty,
        ..setup
    };
    let waiting = replay(&CONSUME_BEFORE_REARM, blocked, &[D]);
    assert_eq!(drain_step(&CONSUME_BEFORE_REARM, blocked, waiting), None);
    let end = replay(
        &CONSUME_BEFORE_REARM,
        blocked,
        &[D, N0, N0, N0, D, N0, D, D, D, D, D],
    );
    assert_eq!(
        end,
        World {
            state: Idle,
            units: 0,
            pending: [0, 0, 0],
            delivered: [0, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done, NotifyAt::Activate],
        }
    );
}
