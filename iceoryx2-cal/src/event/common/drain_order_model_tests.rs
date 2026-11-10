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
//!
//! [`VERDICTS`] is the committed table of what the search finds for every drain order in every
//! setup. [`WITNESSES`] are hand-written schedules that the tests below replay to one end each.
//! The seam test replays the same schedules, and scripts of its own, on the real drain, and
//! checks that the real drain ends where [`predict()`] says the model of [`DRAIN_EVENTS_ORDER`]
//! ends.

use alloc::collections::BTreeSet;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

/// Event ids 0 to 2: the start state activates id 0, the notifiers use ids 1 and 2 or both id 1.
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
pub(super) enum TriggerRead {
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
    /// `empty_buffer()`: one take from the trigger.
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
use Start::{IdleEmpty, IdleStrayUnit, NotifiedOneUnit, NotifiedTwoUnits};
use TriggerRead::{EveryUnit, OneUnit};
use WaitKind::{Blocking, Try};

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
const DRAIN_EVENTS_ORDER: &DrainOrder = &CONSUME_BEFORE_REARM;

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

/// The state a setup starts the drain in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Start {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// Who takes the next step of a schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Actor {
    Drain,
    Notifier(usize),
}

pub(super) const D: Actor = Actor::Drain;
pub(super) const N0: Actor = Actor::Notifier(0);
pub(super) const N1: Actor = Actor::Notifier(1);

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

fn arm(order: &DrainOrder, notified: bool) -> &'static [DrainStep] {
    if notified {
        order.notified_arm
    } else {
        order.other_arm
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
            let arm = arm(order, notified);
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

const PASSES: Verdict = Verdict {
    deaf: false,
    lost_wake: false,
};
const DEAF: Verdict = Verdict {
    deaf: true,
    lost_wake: false,
};
const LOSES_A_WAKE: Verdict = Verdict {
    deaf: false,
    lost_wake: true,
};
const DEAF_AND_LOSES_A_WAKE: Verdict = Verdict {
    deaf: true,
    lost_wake: true,
};

/// The drain orders the table below gives a verdict for, in its column order.
const ORDERS: [&DrainOrder; 4] = [
    &REARM_BEFORE_CONSUME,
    &REARM_AFTER_DRAINS_IF_NOTIFIED,
    &REARM_AFTER_DRAINS,
    &CONSUME_BEFORE_REARM,
];

/// What the exhaustive search must find in one setup, one verdict per order of [`ORDERS`].
struct SetupVerdicts {
    setup: Setup,
    verdicts: [Verdict; 4],
}

const fn row(
    start: Start,
    trigger: TriggerRead,
    wait: WaitKind,
    ids: [usize; 2],
    verdicts: [Verdict; 4],
) -> SetupVerdicts {
    SetupVerdicts {
        setup: Setup {
            start,
            trigger,
            wait,
            ids,
        },
        verdicts,
    }
}

const SEND_IN_THE_WINDOW: [Verdict; 4] = [DEAF, DEAF_AND_LOSES_A_WAKE, LOSES_A_WAKE, PASSES];
const WAKE_FROM_EMPTY: [Verdict; 4] = [DEAF, DEAF, PASSES, PASSES];
const ONE_UNIT_LEFT: [Verdict; 4] = [PASSES, PASSES, PASSES, PASSES];

// Why each row reads as it does:
//
//  * Rearm before consume goes deaf when a notify claims and sends between the re-arm and the
//    trigger read that takes its unit, and marks notified after it. The arm that waits always
//    has that window. The notified arm re-arms in its swap, and its single read empties the
//    trigger only when the read takes every unit: a one-unit read leaves the start's unit or
//    the notify's unit behind. Its one re-arm comes before the event state drain, so a notify
//    that skipped its send on the notified state before the re-arm is delivered by that drain,
//    and one that skipped after it left the state notified: it never loses a wake.
//  * The swap of notified for idle after both drains is deaf in the same setups, as its swap
//    misses a mark that lands after it. It also loses a wake: a notify that activates after
//    the event state drain skips its send on the notified state, and the swap re-arms over
//    it. That needs an empty trigger at the end, so the drain must take the unit of the notify
//    that marked notified after the first re-arm. A blocking wait from an idle empty start
//    returns only on the unit of a notify that sent before it, and the re-arm after the wait
//    clears or fails that notify's mark, so only the notify that would skip is left to mark
//    notified. A one-unit read from a notified start always leaves a unit.
//  * An idle store after both drains loses the same wakes and is never deaf: a notify that marks
//    notified after the store claimed after it, so its unit comes after every read.
//  * Consume before rearm passes in every setup.
const VERDICTS: [SetupVerdicts; 32] = [
    row(IdleEmpty, OneUnit, Try, [1, 2], SEND_IN_THE_WINDOW),
    row(IdleEmpty, OneUnit, Try, [1, 1], SEND_IN_THE_WINDOW),
    row(IdleEmpty, OneUnit, Blocking, [1, 2], WAKE_FROM_EMPTY),
    row(IdleEmpty, OneUnit, Blocking, [1, 1], WAKE_FROM_EMPTY),
    row(IdleEmpty, EveryUnit, Try, [1, 2], SEND_IN_THE_WINDOW),
    row(IdleEmpty, EveryUnit, Try, [1, 1], SEND_IN_THE_WINDOW),
    row(IdleEmpty, EveryUnit, Blocking, [1, 2], WAKE_FROM_EMPTY),
    row(IdleEmpty, EveryUnit, Blocking, [1, 1], WAKE_FROM_EMPTY),
    row(IdleStrayUnit, OneUnit, Try, [1, 2], SEND_IN_THE_WINDOW),
    row(IdleStrayUnit, OneUnit, Try, [1, 1], SEND_IN_THE_WINDOW),
    row(IdleStrayUnit, OneUnit, Blocking, [1, 2], SEND_IN_THE_WINDOW),
    row(IdleStrayUnit, OneUnit, Blocking, [1, 1], SEND_IN_THE_WINDOW),
    row(IdleStrayUnit, EveryUnit, Try, [1, 2], SEND_IN_THE_WINDOW),
    row(IdleStrayUnit, EveryUnit, Try, [1, 1], SEND_IN_THE_WINDOW),
    row(
        IdleStrayUnit,
        EveryUnit,
        Blocking,
        [1, 2],
        SEND_IN_THE_WINDOW,
    ),
    row(
        IdleStrayUnit,
        EveryUnit,
        Blocking,
        [1, 1],
        SEND_IN_THE_WINDOW,
    ),
    row(NotifiedOneUnit, OneUnit, Try, [1, 2], ONE_UNIT_LEFT),
    row(NotifiedOneUnit, OneUnit, Try, [1, 1], ONE_UNIT_LEFT),
    row(NotifiedOneUnit, OneUnit, Blocking, [1, 2], ONE_UNIT_LEFT),
    row(NotifiedOneUnit, OneUnit, Blocking, [1, 1], ONE_UNIT_LEFT),
    row(NotifiedOneUnit, EveryUnit, Try, [1, 2], SEND_IN_THE_WINDOW),
    row(NotifiedOneUnit, EveryUnit, Try, [1, 1], SEND_IN_THE_WINDOW),
    row(
        NotifiedOneUnit,
        EveryUnit,
        Blocking,
        [1, 2],
        SEND_IN_THE_WINDOW,
    ),
    row(
        NotifiedOneUnit,
        EveryUnit,
        Blocking,
        [1, 1],
        SEND_IN_THE_WINDOW,
    ),
    row(NotifiedTwoUnits, OneUnit, Try, [1, 2], ONE_UNIT_LEFT),
    row(NotifiedTwoUnits, OneUnit, Try, [1, 1], ONE_UNIT_LEFT),
    row(NotifiedTwoUnits, OneUnit, Blocking, [1, 2], ONE_UNIT_LEFT),
    row(NotifiedTwoUnits, OneUnit, Blocking, [1, 1], ONE_UNIT_LEFT),
    row(NotifiedTwoUnits, EveryUnit, Try, [1, 2], SEND_IN_THE_WINDOW),
    row(NotifiedTwoUnits, EveryUnit, Try, [1, 1], SEND_IN_THE_WINDOW),
    row(
        NotifiedTwoUnits,
        EveryUnit,
        Blocking,
        [1, 2],
        SEND_IN_THE_WINDOW,
    ),
    row(
        NotifiedTwoUnits,
        EveryUnit,
        Blocking,
        [1, 1],
        SEND_IN_THE_WINDOW,
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
    start: IdleEmpty,
    trigger: OneUnit,
    wait: Try,
    ids: [1, 2],
};

const STREAM_TRY_FROM_NOTIFIED: Setup = Setup {
    start: NotifiedOneUnit,
    trigger: EveryUnit,
    wait: Try,
    ids: [1, 2],
};

const DATAGRAM_TRY_FROM_NOTIFIED: Setup = Setup {
    trigger: OneUnit,
    ..STREAM_TRY_FROM_NOTIFIED
};

/// A hand-written schedule of one drain order in one setup.
pub(super) struct ModelWitness {
    pub(super) name: &'static str,
    order: &'static DrainOrder,
    setup: Setup,
    schedule: &'static [Actor],
}

impl ModelWitness {
    pub(super) fn start(&self) -> Start {
        self.setup.start
    }

    pub(super) fn waits_with_try(&self) -> bool {
        self.setup.wait == Try
    }

    pub(super) fn notifiers_share_an_event_id(&self) -> bool {
        self.setup.ids[0] == self.setup.ids[1]
    }

    /// The schedule with one drain step for each step boundary of the real drain: the model
    /// drains the event state one id per step, the real drain in one call.
    pub(super) fn boundary_schedule(&self) -> Vec<Actor> {
        let mut world = self.setup.start.world();
        let mut schedule = Vec::new();
        for (n, actor) in self.schedule.iter().enumerate() {
            if !(*actor == D && is_inside_the_event_state_drain(self.order, world)) {
                schedule.push(*actor);
            }
            world = step(self.order, self.setup, world, *actor)
                .unwrap_or_else(|| panic!("step {n} ({actor:?}) is not enabled in {world:?}"));
        }
        schedule
    }

    fn end(&self) -> World {
        replay(self.order, self.setup, self.schedule)
    }
}

pub(super) const REARM_BEFORE_CONSUME_GOES_DEAF: ModelWitness = ModelWitness {
    name: "rearm before consume goes deaf when a notify sends between the rearm and the consume",
    order: &REARM_BEFORE_CONSUME,
    setup: DATAGRAM_TRY_FROM_IDLE,
    schedule: &[D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1],
};

pub(super) const CONSUME_BEFORE_REARM_KEEPS_THE_WAKE: ModelWitness = ModelWitness {
    name: "consume before rearm keeps the wake of a notify that sends during the drain",
    order: &CONSUME_BEFORE_REARM,
    setup: DATAGRAM_TRY_FROM_IDLE,
    schedule: &[D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1, N1, N1],
};

pub(super) const REARM_AFTER_DRAINS_IF_NOTIFIED_GOES_DEAF: ModelWitness = ModelWitness {
    name: "rearm after drains if notified goes deaf when the notifier marks after the swap",
    order: &REARM_AFTER_DRAINS_IF_NOTIFIED,
    setup: DATAGRAM_TRY_FROM_IDLE,
    schedule: &[D, D, D, N0, N0, N0, D, D, D, D, D, N0, N1, N1],
};

pub(super) const REARM_AFTER_DRAINS_IF_NOTIFIED_LOSES_A_WAKE: ModelWitness = ModelWitness {
    name: "rearm after drains if notified loses the wake of a notify that skipped on notified",
    order: &REARM_AFTER_DRAINS_IF_NOTIFIED,
    setup: DATAGRAM_TRY_FROM_IDLE,
    schedule: &[D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1, D],
};

pub(super) const REARM_AFTER_DRAINS_LOSES_A_WAKE: ModelWitness = ModelWitness {
    name: "rearm after drains loses the wake of a notify that skipped on notified",
    order: &REARM_AFTER_DRAINS,
    setup: DATAGRAM_TRY_FROM_IDLE,
    schedule: &[D, D, D, N0, N0, N0, D, N0, D, D, D, N1, N1, D],
};

pub(super) const THE_SWAP_GOES_DEAF_ON_A_READ_OF_EVERY_UNIT: ModelWitness = ModelWitness {
    name: "rearm before consume goes deaf when a notify sends after the swap and the read takes every unit",
    order: &REARM_BEFORE_CONSUME,
    setup: STREAM_TRY_FROM_NOTIFIED,
    schedule: &[D, N0, N0, N0, D, D, D, D, N0, N1, N1],
};

pub(super) const THE_SWAP_KEEPS_A_UNIT_ON_A_ONE_UNIT_READ: ModelWitness = ModelWitness {
    name: "rearm before consume keeps a unit after the swap when the read takes one unit",
    order: &REARM_BEFORE_CONSUME,
    setup: DATAGRAM_TRY_FROM_NOTIFIED,
    schedule: &[D, N0, N0, N0, D, D, D, D, N0, N1, N1],
};

/// Every witness the tests below replay.
pub(super) const WITNESSES: [&ModelWitness; 7] = [
    &REARM_BEFORE_CONSUME_GOES_DEAF,
    &CONSUME_BEFORE_REARM_KEEPS_THE_WAKE,
    &REARM_AFTER_DRAINS_IF_NOTIFIED_GOES_DEAF,
    &REARM_AFTER_DRAINS_IF_NOTIFIED_LOSES_A_WAKE,
    &REARM_AFTER_DRAINS_LOSES_A_WAKE,
    &THE_SWAP_GOES_DEAF_ON_A_READ_OF_EVERY_UNIT,
    &THE_SWAP_KEEPS_A_UNIT_ON_A_ONE_UNIT_READ,
];

/// Whether the drain is past the first event id of its event state drain.
fn is_inside_the_event_state_drain(order: &DrainOrder, world: World) -> bool {
    match world.drain {
        DrainAt::Arm {
            notified,
            step,
            event_id,
        } => event_id > 0 && arm(order, notified)[step] == DrainEventState,
        DrainAt::Check | DrainAt::Done => false,
    }
}

/// One step of the real drain: the model's next step, with the event state drained for every
/// id at once.
fn real_drain_step(order: &DrainOrder, setup: Setup, world: World) -> Option<World> {
    let mut next = drain_step(order, setup, world)?;
    while is_inside_the_event_state_drain(order, next) {
        next = drain_step(order, setup, next)?;
    }
    Some(next)
}

/// Where the model of [`DRAIN_EVENTS_ORDER`] ends a seam script.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Prediction {
    pub(super) notified: bool,
    pub(super) units: u8,
    /// Activations the drain delivered, per event id: 0 for the start's notify, 1 and 2 for
    /// notifiers 0 and 1.
    pub(super) delivered: [u8; EVENT_IDS],
}

/// Runs `script` against [`DRAIN_EVENTS_ORDER`] as the seam test runs it against the real drain,
/// with a try wait and a distinct event id per notifier. A notifier step runs at once and does
/// nothing once that notify ended. A drain step runs the drain's next step, the event state
/// drain as one step, and does nothing once the drain is done. The drain then runs to its end,
/// and every notifier the script names runs to the end of its notify, notifier 0 first.
pub(super) fn predict(start: Start, trigger: TriggerRead, script: &[Actor]) -> Prediction {
    let order = DRAIN_EVENTS_ORDER;
    let setup = Setup {
        start,
        trigger,
        wait: Try,
        ids: [1, 2],
    };
    let mut world = start.world();
    for actor in script {
        let next = match actor {
            Actor::Drain => real_drain_step(order, setup, world),
            Actor::Notifier(n) => notify_step(setup, world, *n),
        };
        world = next.unwrap_or(world);
    }
    while let Some(next) = real_drain_step(order, setup, world) {
        world = next;
    }
    for notifier in 0..2 {
        if script.contains(&Actor::Notifier(notifier)) {
            while let Some(next) = notify_step(setup, world, notifier) {
                world = next;
            }
        }
    }
    Prediction {
        notified: world.state == Notified,
        units: world.units,
        delivered: world.delivered,
    }
}

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
fn the_model_explores_32_setups_and_the_table_lists_each_once_in_their_order() {
    let setups = every_setup();
    assert_eq!(setups.len(), 32);
    let listed: Vec<Setup> = VERDICTS.iter().map(|row| row.setup).collect();
    assert_eq!(listed, setups);
}

#[test]
fn every_drain_order_matches_its_committed_verdict_in_every_setup() {
    let mut mismatches = Vec::new();
    for row in &VERDICTS {
        for (order, expected) in ORDERS.iter().zip(row.verdicts) {
            let findings = explore(order, row.setup);
            assert!(findings.worlds_seen > 0 && !findings.ends.is_empty());
            let found = findings.verdict();
            if found != expected {
                mismatches.push(format!(
                    "{}: {:?}: the search finds {found:?}, the table says {expected:?}",
                    order.name, row.setup
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

#[test]
fn the_committed_table_fails_every_order_but_consume_before_rearm() {
    // (setups deaf, setups that lose a wake) per order of ORDERS.
    let expected = [(24, 0), (24, 20), (0, 20), (0, 0)];
    for (column, order) in ORDERS.iter().enumerate() {
        let deaf = VERDICTS
            .iter()
            .filter(|row| row.verdicts[column].deaf)
            .count();
        let lost_wake = VERDICTS
            .iter()
            .filter(|row| row.verdicts[column].lost_wake)
            .count();
        assert_eq!((deaf, lost_wake), expected[column], "{}", order.name);
    }
    assert_eq!(ORDERS[3].name, CONSUME_BEFORE_REARM.name);
}

#[test]
fn rearm_before_consume_goes_deaf_when_a_notify_sends_between_the_rearm_and_the_consume() {
    let witness = &REARM_BEFORE_CONSUME_GOES_DEAF;
    let end = witness.end();

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
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(end.is_deaf());
    assert!(explore(witness.order, witness.setup).ends.contains(&end));
}

#[test]
fn consume_before_rearm_keeps_the_wake_of_a_notify_that_sends_during_the_drain() {
    let witness = &CONSUME_BEFORE_REARM_KEEPS_THE_WAKE;
    let end = witness.end();

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
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(!end.is_deaf() && !end.loses_a_wake());
    assert!(explore(witness.order, witness.setup).ends.contains(&end));
}

#[test]
fn rearm_after_drains_if_notified_goes_deaf_when_the_notifier_marks_after_the_swap() {
    let witness = &REARM_AFTER_DRAINS_IF_NOTIFIED_GOES_DEAF;
    let end = witness.end();

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
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(end.is_deaf());
}

#[test]
fn rearm_after_drains_if_notified_loses_the_wake_of_a_notify_that_skipped_on_notified() {
    let witness = &REARM_AFTER_DRAINS_IF_NOTIFIED_LOSES_A_WAKE;
    let end = witness.end();

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
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(end.loses_a_wake());
}

#[test]
fn rearm_after_drains_loses_the_wake_of_a_notify_that_skipped_on_notified() {
    let witness = &REARM_AFTER_DRAINS_LOSES_A_WAKE;
    let end = witness.end();

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
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(end.loses_a_wake());
}

#[test]
fn rearm_before_consume_goes_deaf_when_a_notify_sends_after_the_swap_and_the_read_takes_every_unit()
{
    let witness = &THE_SWAP_GOES_DEAF_ON_A_READ_OF_EVERY_UNIT;
    let end = witness.end();

    assert_eq!(
        end,
        World {
            state: Notified,
            units: 0,
            pending: [0, 0, 1],
            delivered: [1, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(end.is_deaf());
    assert!(explore(witness.order, witness.setup).ends.contains(&end));
}

#[test]
fn rearm_before_consume_keeps_a_unit_after_the_swap_when_the_read_takes_one_unit() {
    let witness = &THE_SWAP_KEEPS_A_UNIT_ON_A_ONE_UNIT_READ;
    let end = witness.end();

    assert_eq!(
        end,
        World {
            state: Notified,
            units: 1,
            pending: [0, 0, 1],
            delivered: [1, 1, 0],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(witness.order, witness.setup, end));
    assert!(!end.is_deaf() && !end.loses_a_wake());
}

#[test]
fn two_notifies_that_find_the_state_pending_both_send_and_a_drain_leaves_one_unit() {
    let schedule = [N0, N0, N1, N1, N1, N0, N0, N1, D, D, D, D, D, D];
    let end = replay(&CONSUME_BEFORE_REARM, DATAGRAM_TRY_FROM_IDLE, &schedule);

    assert_eq!(
        end,
        World {
            state: Idle,
            units: 1,
            pending: [0, 0, 0],
            delivered: [0, 1, 1],
            drain: DrainAt::Done,
            notifiers: [NotifyAt::Done; 2],
        }
    );
    assert!(is_quiescent(
        &CONSUME_BEFORE_REARM,
        DATAGRAM_TRY_FROM_IDLE,
        end
    ));
    assert_eq!(end.state, IdleStrayUnit.world().state);
    assert_eq!(end.units, IdleStrayUnit.world().units);
}

#[test]
fn a_witness_schedule_takes_one_drain_step_per_boundary_of_the_real_drain() {
    let expected: [&[Actor]; 7] = [
        &[D, D, D, N0, N0, N0, D, N0, D, N1, N1],
        &[D, D, D, N0, N0, N0, D, N0, D, N1, N1, N1, N1],
        &[D, D, D, N0, N0, N0, D, D, D, N0, N1, N1],
        &[D, D, D, N0, N0, N0, D, N0, D, N1, N1, D],
        &[D, D, D, N0, N0, N0, D, N0, D, N1, N1, D],
        &[D, N0, N0, N0, D, D, N0, N1, N1],
        &[D, N0, N0, N0, D, D, N0, N1, N1],
    ];
    for (witness, expected) in WITNESSES.iter().zip(expected) {
        assert_eq!(witness.boundary_schedule(), expected, "{}", witness.name);
    }
}

#[test]
fn the_prediction_runs_a_script_at_the_real_drain_boundaries_of_consume_before_rearm() {
    // A notify that sends after the trigger read and marks after the re-arm leaves its unit.
    assert_eq!(
        predict(IdleEmpty, OneUnit, &[D, D, D, N0, N0, N0, D, N0, D, N1, N1]),
        Prediction {
            notified: true,
            units: 2,
            delivered: [0, 1, 0],
        }
    );
    // On a notified start the notify skips on the state the load left notified, and the
    // second notify claims the idle state after the re-arm, before the event state drain.
    assert_eq!(
        predict(
            NotifiedOneUnit,
            EveryUnit,
            &[D, N0, N0, N0, D, D, N0, N1, N1]
        ),
        Prediction {
            notified: true,
            units: 1,
            delivered: [1, 1, 1],
        }
    );
    // Drain steps past the end of the drain do nothing. The notifies the script started finish
    // after it: the first claims and sends, the second skips on the notified state.
    assert_eq!(
        predict(IdleStrayUnit, OneUnit, &[D, D, D, D, D, D, D, D, N1, N0]),
        Prediction {
            notified: true,
            units: 1,
            delivered: [0, 0, 0],
        }
    );
}

#[test]
fn a_blocking_drain_waits_for_a_unit_and_a_stream_read_takes_every_unit() {
    let setup = Setup {
        start: IdleStrayUnit,
        trigger: EveryUnit,
        wait: Blocking,
        ids: [1, 2],
    };
    let after_wait = replay(&CONSUME_BEFORE_REARM, setup, &[N0, N0, N0, D, D]);
    assert_eq!(after_wait.units, 0);
    assert_eq!(after_wait.state, Pending);

    let blocked = Setup {
        start: IdleEmpty,
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
