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

//! Runs notifies inside a real drain, at its step boundaries, and checks that the listener
//! still wakes afterwards.
//!
//! In every cell a listener and a notifier are created over a real trigger and set up in one
//! start state, and the drain hook runs notify steps when the `try_wait()` drain reaches a
//! step boundary.
//!
//! The first table crosses the start states with the boundaries and runs one complete
//! `notify(HOOKED)` at the cell's boundary. Each cell checks:
//!
//!  * the drain reached the boundary once when the oracle says it passes it, else never,
//!  * the drain delivered the activations of the cell's oracle row,
//!  * the drain did not end with the state notified and an empty trigger, the state in which
//!    every later notify skips its send,
//!  * an activation the drain left behind has a wake waiting in the trigger,
//!  * a later `notify(LATER)` makes the trigger readable, and the next drain delivers the
//!    activations of the oracle row.
//!
//! The scripted cells run a notify one step at a time, the steps of `Handle::notify()`, at
//! positions in the drain: before its first step, after any of its steps, or after it
//! returned. They place the four steps of one notify at every combination of positions, two
//! complete notifies at every pair of positions, and the witness schedules of the interleaving
//! model. Each scripted cell checks that the drain does not end notified with an empty
//! trigger, that an activation it left behind has a wake waiting, that a later notify makes the
//! trigger readable, that the two drains deliver every activation once, and that the drain ends
//! where the model of the order `drain_events` implements says it ends. A witness cell also
//! checks the end of its hand-written oracle row.

use super::drain_gap::{self, DrainGap};
use super::drain_order_model_tests::{
    self as model, Actor, D, ModelWitness, N0, N1, TriggerRead, WITNESSES,
};
use super::{
    EventImpl, Handle, HandleBuilder, NOTIFICATION_STATE_IDLE, NOTIFICATION_STATE_NOTIFIED,
    NOTIFICATION_STATE_PENDING, Waiter, WaiterBuilder,
};
use crate::dynamic_storage::DynamicStorage;
use crate::event::event_state::EventState;
use crate::event::trigger::{HandlerInterface, State, WaiterInterface};
use crate::event::{
    EventId, Listener, ListenerBuilder, NamedConceptBuilder, Notifier, NotifierBuilder,
    SocketPairCountingBitSet, UnixDatagramShmBitSet, UnixDatagramShmCountingBitSet,
};
use crate::testing::generate_isolated_config;
use DrainGap::{
    AfterEmptyBuffer, AfterEventStateDrain, AfterRearm, AfterStateCheck, AfterWaitCall, Entry,
};
use Start::{IdleEmpty, IdleStrayByte, NotifiedOneByte, NotifiedTwoBytes};
use alloc::boxed::Box;
use alloc::format;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::marker::PhantomData;
use core::time::Duration;
use iceoryx2_bb_concurrency::atomic::Ordering;
use iceoryx2_bb_concurrency::cell::{Cell, RefCell};
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_bb_posix::file_descriptor_set::{
    FileDescriptorSet, FileEvent, SynchronousMultiplexing,
};
use iceoryx2_bb_posix::testing::generate_file_name;

/// The event id a notify sent before the drain activated.
const PRIOR: usize = 1;
/// The event id the hook notifies inside the drain.
const HOOKED: usize = 2;
/// The event id notified after the drain returned.
const LATER: usize = 3;
/// The event id of the second notifier a script runs.
const SECOND: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    /// Idle state, empty trigger, no activation.
    IdleEmpty,
    /// Idle state with one trigger byte and no activation: what a drain leaves when two
    /// notifies both sent while pending.
    IdleStrayByte,
    /// One completed `notify(PRIOR)`: notified state, one trigger byte.
    NotifiedOneByte,
    /// One completed `notify(PRIOR)` plus one more trigger byte: notified state, two bytes.
    NotifiedTwoBytes,
}

const STARTS: [Start; 4] = [IdleEmpty, IdleStrayByte, NotifiedOneByte, NotifiedTwoBytes];

impl Start {
    fn model(self) -> model::Start {
        match self {
            IdleEmpty => model::Start::IdleEmpty,
            IdleStrayByte => model::Start::IdleStrayUnit,
            NotifiedOneByte => model::Start::NotifiedOneUnit,
            NotifiedTwoBytes => model::Start::NotifiedTwoUnits,
        }
    }

    fn of_model(start: model::Start) -> Self {
        match start {
            model::Start::IdleEmpty => IdleEmpty,
            model::Start::IdleStrayUnit => IdleStrayByte,
            model::Start::NotifiedOneUnit => NotifiedOneByte,
            model::Start::NotifiedTwoUnits => NotifiedTwoBytes,
        }
    }

    fn is_notified(self) -> bool {
        match self {
            IdleEmpty | IdleStrayByte => false,
            NotifiedOneByte | NotifiedTwoBytes => true,
        }
    }
}

/// The hand-written oracle of one cell.
struct Oracle {
    start: Start,
    gap: DrainGap,
    /// Whether the drain passes `gap`, so the hook runs.
    passes_boundary: bool,
    /// The event ids the hooked drain delivers, ascending, each activated once.
    this_drain: &'static [usize],
    /// The event ids the drain after `notify(LATER)` delivers, ascending.
    next_drain: &'static [usize],
}

const fn cell(
    start: Start,
    gap: DrainGap,
    passes_boundary: bool,
    this_drain: &'static [usize],
    next_drain: &'static [usize],
) -> Oracle {
    Oracle {
        start,
        gap,
        passes_boundary,
        this_drain,
        next_drain,
    }
}

// A drain of a notified listener does not call the trigger wait, so it never reaches
// `AfterWaitCall`. The event state is drained last, so the hooked activation is delivered by
// the hooked drain itself unless the hook runs after the event state drain.
const ORACLE: [Oracle; 24] = [
    cell(IdleEmpty, Entry, true, &[HOOKED], &[LATER]),
    cell(IdleEmpty, AfterStateCheck, true, &[HOOKED], &[LATER]),
    cell(IdleEmpty, AfterWaitCall, true, &[HOOKED], &[LATER]),
    cell(IdleEmpty, AfterRearm, true, &[HOOKED], &[LATER]),
    cell(IdleEmpty, AfterEmptyBuffer, true, &[HOOKED], &[LATER]),
    cell(IdleEmpty, AfterEventStateDrain, true, &[], &[HOOKED, LATER]),
    cell(IdleStrayByte, Entry, true, &[HOOKED], &[LATER]),
    cell(IdleStrayByte, AfterStateCheck, true, &[HOOKED], &[LATER]),
    cell(IdleStrayByte, AfterWaitCall, true, &[HOOKED], &[LATER]),
    cell(IdleStrayByte, AfterRearm, true, &[HOOKED], &[LATER]),
    cell(IdleStrayByte, AfterEmptyBuffer, true, &[HOOKED], &[LATER]),
    cell(
        IdleStrayByte,
        AfterEventStateDrain,
        true,
        &[],
        &[HOOKED, LATER],
    ),
    cell(NotifiedOneByte, Entry, true, &[PRIOR, HOOKED], &[LATER]),
    cell(
        NotifiedOneByte,
        AfterStateCheck,
        true,
        &[PRIOR, HOOKED],
        &[LATER],
    ),
    cell(NotifiedOneByte, AfterWaitCall, false, &[PRIOR], &[LATER]),
    cell(
        NotifiedOneByte,
        AfterRearm,
        true,
        &[PRIOR, HOOKED],
        &[LATER],
    ),
    cell(
        NotifiedOneByte,
        AfterEmptyBuffer,
        true,
        &[PRIOR, HOOKED],
        &[LATER],
    ),
    cell(
        NotifiedOneByte,
        AfterEventStateDrain,
        true,
        &[PRIOR],
        &[HOOKED, LATER],
    ),
    cell(NotifiedTwoBytes, Entry, true, &[PRIOR, HOOKED], &[LATER]),
    cell(
        NotifiedTwoBytes,
        AfterStateCheck,
        true,
        &[PRIOR, HOOKED],
        &[LATER],
    ),
    cell(NotifiedTwoBytes, AfterWaitCall, false, &[PRIOR], &[LATER]),
    cell(
        NotifiedTwoBytes,
        AfterRearm,
        true,
        &[PRIOR, HOOKED],
        &[LATER],
    ),
    cell(
        NotifiedTwoBytes,
        AfterEmptyBuffer,
        true,
        &[PRIOR, HOOKED],
        &[LATER],
    ),
    cell(
        NotifiedTwoBytes,
        AfterEventStateDrain,
        true,
        &[PRIOR],
        &[HOOKED, LATER],
    ),
];

fn is_readable<T: SynchronousMultiplexing>(fd: &T) -> bool {
    let set = FileDescriptorSet::new();
    let _attachment = set.add(fd).unwrap();
    set.timed_wait(Duration::ZERO, FileEvent::Read, |_| {})
        .unwrap()
        == 1
}

fn once_each(ids: &[usize]) -> Vec<(usize, u64)> {
    ids.iter().map(|id| (*id, 1)).collect()
}

fn sorted(mut activations: Vec<(usize, u64)>) -> Vec<(usize, u64)> {
    activations.sort_unstable();
    activations
}

fn state_of<E, Mgmt, Storage, W>(listener: &Waiter<E, Mgmt, Storage, W>) -> u8
where
    E: EventState,
    Mgmt: ZeroCopySend + Send + Sync + Debug,
    Storage: DynamicStorage<State<E, Mgmt>>,
    W: WaiterInterface<E, Mgmt, Storage>,
{
    listener
        .storage
        .get()
        .notification_state
        .load(Ordering::SeqCst)
}

/// A listener and the notifier that notifies it.
type ListenerAndNotifier<E, Mgmt, Storage, H, W> =
    (Waiter<E, Mgmt, Storage, W>, Rc<Handle<E, Mgmt, Storage, H>>);

/// Creates a listener and a notifier over a real trigger and sets up `start`.
fn listener_in<E, Mgmt, Storage, H, W>(
    start: Start,
    cell: &str,
) -> ListenerAndNotifier<E, Mgmt, Storage, H, W>
where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let name = generate_file_name();
    let config = generate_isolated_config::<EventImpl<E, Mgmt, Storage, H, W>>();
    let listener = WaiterBuilder::<E, Mgmt, Storage, H, W>::new(&name)
        .config(&config)
        .create()
        .unwrap();
    let notifier = Rc::new(
        HandleBuilder::<E, Mgmt, Storage, H, W>::new(&name)
            .config(&config)
            .open()
            .unwrap(),
    );

    match start {
        IdleEmpty => (),
        IdleStrayByte => notifier.handle.notify().unwrap(),
        NotifiedOneByte => notifier.notify(EventId::new(PRIOR)).unwrap(),
        NotifiedTwoBytes => {
            notifier.notify(EventId::new(PRIOR)).unwrap();
            notifier.handle.notify().unwrap();
        }
    }
    let start_state = if start.is_notified() {
        NOTIFICATION_STATE_NOTIFIED
    } else {
        NOTIFICATION_STATE_IDLE
    };
    assert_eq!(state_of(&listener), start_state, "{cell}: start state");
    assert_eq!(
        is_readable(&listener),
        start != IdleEmpty,
        "{cell}: start trigger"
    );
    (listener, notifier)
}

/// Runs one cell and returns every check it fails, empty when it passes.
fn run_cell<E, Mgmt, Storage, H, W>(oracle: &Oracle) -> Vec<String>
where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let cell = format!("cell ({:?}, {:?})", oracle.start, oracle.gap);
    let mut failures = Vec::new();
    let (listener, notifier) = listener_in::<E, Mgmt, Storage, H, W>(oracle.start, &cell);

    let boundary_reached = Rc::new(Cell::new(0u32));
    let guard = {
        let target = oracle.gap;
        let boundary_reached = boundary_reached.clone();
        let notifier = notifier.clone();
        drain_gap::install(Box::new(move |gap| {
            if gap != target {
                return;
            }
            boundary_reached.set(boundary_reached.get() + 1);
            if boundary_reached.get() == 1 {
                notifier.notify(EventId::new(HOOKED)).unwrap();
            }
        }))
    };
    let mut this_drain = Vec::new();
    listener
        .try_wait(|activation| this_drain.push((activation.id.as_value(), activation.count)))
        .unwrap();
    drop(guard);

    if boundary_reached.get() != u32::from(oracle.passes_boundary) {
        failures.push(format!(
            "{cell}: the drain reached the boundary {} times, the oracle says {}",
            boundary_reached.get(),
            u32::from(oracle.passes_boundary)
        ));
    }
    let this_drain = sorted(this_drain);
    if this_drain != once_each(oracle.this_drain) {
        failures.push(format!(
            "{cell}: the drain delivered {this_drain:?}, the oracle says {:?}",
            once_each(oracle.this_drain)
        ));
    }

    let notified_after = state_of(&listener) == NOTIFICATION_STATE_NOTIFIED;
    let readable_after = is_readable(&listener);
    if notified_after && !readable_after {
        failures.push(format!(
            "{cell}: the drain ended with the state notified and an empty trigger"
        ));
    }
    if oracle.next_drain.contains(&HOOKED) && !readable_after {
        failures.push(format!(
            "{cell}: the drain left the hooked activation with no wake in the trigger"
        ));
    }

    notifier.notify(EventId::new(LATER)).unwrap();
    if !is_readable(&listener) {
        failures.push(format!(
            "{cell}: a notify after the drain left the trigger unreadable"
        ));
    }
    let mut next_drain = Vec::new();
    listener
        .try_wait(|activation| next_drain.push((activation.id.as_value(), activation.count)))
        .unwrap();
    let next_drain = sorted(next_drain);
    if next_drain != once_each(oracle.next_drain) {
        failures.push(format!(
            "{cell}: the next drain delivered {next_drain:?}, the oracle says {:?}",
            once_each(oracle.next_drain)
        ));
    }

    failures
}

fn run_every_cell<E, Mgmt, Storage, H, W>(_event: PhantomData<EventImpl<E, Mgmt, Storage, H, W>>)
where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let mut cells_run = 0;
    let mut failures = Vec::new();
    for oracle in &ORACLE {
        failures.extend(run_cell::<E, Mgmt, Storage, H, W>(oracle));
        cells_run += 1;
    }
    assert_eq!(cells_run, 24);
    assert!(
        failures.is_empty(),
        "{} failed checks:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn a_notify_at_any_step_of_a_drain_leaves_a_unix_datagram_counting_listener_awake() {
    run_every_cell(PhantomData::<UnixDatagramShmCountingBitSet>);
}

#[test]
fn a_notify_at_any_step_of_a_drain_leaves_a_unix_datagram_bitset_listener_awake() {
    run_every_cell(PhantomData::<UnixDatagramShmBitSet>);
}

#[test]
fn a_notify_at_any_step_of_a_drain_leaves_a_socket_pair_counting_listener_awake() {
    run_every_cell(PhantomData::<SocketPairCountingBitSet>);
}

/// The last position a script places a notify step at. Position `k` is the step boundary
/// after the drain's `k`-th step, position 0 the boundary before its first step. A drain takes
/// at most five steps (on the arm that waits: the state check, the wait, the trigger read, the
/// re-arm and the event state drain), so position 6 is after the drain returned.
const LAST_POSITION: usize = 6;

/// A notify run one step at a time, in the steps of `Handle::notify()`: activate the event id,
/// claim the pending state from idle (a notified state ends the notify, a pending state sends
/// anyway), send one trigger unit, and mark the pending state notified.
struct SteppedNotify {
    id: usize,
    steps_run: usize,
    ended_on_notified: bool,
}

impl SteppedNotify {
    const STEPS: usize = 4;

    const fn new(id: usize) -> Self {
        Self {
            id,
            steps_run: 0,
            ended_on_notified: false,
        }
    }

    fn is_finished(&self) -> bool {
        self.ended_on_notified || self.steps_run == Self::STEPS
    }

    fn run_next_step<E, Mgmt, Storage, H>(&mut self, notifier: &Handle<E, Mgmt, Storage, H>)
    where
        E: EventState,
        Mgmt: ZeroCopySend + Send + Sync + Debug,
        Storage: DynamicStorage<State<E, Mgmt>>,
        H: HandlerInterface<E, Mgmt, Storage>,
    {
        if self.is_finished() {
            return;
        }
        let mgmt = notifier.storage.get();
        match self.steps_run {
            0 => mgmt.event.activate(EventId::new(self.id)).unwrap(),
            1 => {
                let claim = mgmt.notification_state.compare_exchange(
                    NOTIFICATION_STATE_IDLE,
                    NOTIFICATION_STATE_PENDING,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                self.ended_on_notified = claim == Err(NOTIFICATION_STATE_NOTIFIED);
            }
            2 => notifier.handle.notify().unwrap(),
            _ => {
                let _ = mgmt.notification_state.compare_exchange(
                    NOTIFICATION_STATE_PENDING,
                    NOTIFICATION_STATE_NOTIFIED,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
        self.steps_run += 1;
    }
}

/// A script of drain and notifier steps, as the model writes its schedules, and how far a
/// drain has run it. Notifier 0 notifies `HOOKED`, notifier 1 `SECOND`.
struct ScriptRun {
    script: Vec<Actor>,
    next: usize,
    notifies: [SteppedNotify; 2],
}

impl ScriptRun {
    fn new(script: &[Actor]) -> Self {
        Self {
            script: script.to_vec(),
            next: 0,
            notifies: [SteppedNotify::new(HOOKED), SteppedNotify::new(SECOND)],
        }
    }

    fn names(&self, notifier: usize) -> bool {
        self.script.contains(&Actor::Notifier(notifier))
    }

    /// Runs the notifier steps up to the script's next drain step, which the drain takes when
    /// this returns.
    fn at_boundary<E, Mgmt, Storage, H>(&mut self, notifier: &Handle<E, Mgmt, Storage, H>)
    where
        E: EventState,
        Mgmt: ZeroCopySend + Send + Sync + Debug,
        Storage: DynamicStorage<State<E, Mgmt>>,
        H: HandlerInterface<E, Mgmt, Storage>,
    {
        while let Some(actor) = self.script.get(self.next).copied() {
            self.next += 1;
            match actor {
                Actor::Drain => return,
                Actor::Notifier(n) => self.notifies[n].run_next_step(notifier),
            }
        }
    }

    /// Runs the notifier steps the drain did not reach, then every notify the script names to
    /// its end, notifier 0 first.
    fn after_drain<E, Mgmt, Storage, H>(&mut self, notifier: &Handle<E, Mgmt, Storage, H>)
    where
        E: EventState,
        Mgmt: ZeroCopySend + Send + Sync + Debug,
        Storage: DynamicStorage<State<E, Mgmt>>,
        H: HandlerInterface<E, Mgmt, Storage>,
    {
        while let Some(actor) = self.script.get(self.next).copied() {
            self.next += 1;
            if let Actor::Notifier(n) = actor {
                self.notifies[n].run_next_step(notifier);
            }
        }
        for n in 0..2 {
            if self.names(n) {
                while !self.notifies[n].is_finished() {
                    self.notifies[n].run_next_step(notifier);
                }
            }
        }
    }
}

/// Where a scripted drain ended.
#[derive(Debug, PartialEq, Eq)]
struct ScriptEnd {
    /// The activations the scripted drain delivered, ascending.
    this_drain: Vec<(usize, u64)>,
    /// Whether the state was notified once every notify of the script finished.
    notified: bool,
    /// Whether the trigger was readable then.
    readable: bool,
    /// The activations the drain after a further `notify(LATER)` delivered, ascending.
    next_drain: Vec<(usize, u64)>,
}

/// Runs `script` on a real drain from `start` and returns where it ended, with every check it
/// fails. `trigger` is how the listener's trigger reads, for the model's prediction.
fn run_script<E, Mgmt, Storage, H, W>(
    cell: &str,
    start: Start,
    script: &[Actor],
    trigger: TriggerRead,
) -> (ScriptEnd, Vec<String>)
where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let mut failures = Vec::new();
    let (listener, notifier) = listener_in::<E, Mgmt, Storage, H, W>(start, cell);

    let run = Rc::new(RefCell::new(ScriptRun::new(script)));
    let guard = {
        let run = run.clone();
        let notifier = notifier.clone();
        drain_gap::install(Box::new(move |_| run.borrow_mut().at_boundary(&notifier)))
    };
    let mut this_drain = Vec::new();
    listener
        .try_wait(|activation| this_drain.push((activation.id.as_value(), activation.count)))
        .unwrap();
    drop(guard);
    run.borrow_mut().after_drain(&notifier);
    let this_drain = sorted(this_drain);
    let notified = state_of(&listener) == NOTIFICATION_STATE_NOTIFIED;
    let readable = is_readable(&listener);

    let mut activated = Vec::new();
    if start.is_notified() {
        activated.push(PRIOR);
    }
    for (n, id) in [HOOKED, SECOND].into_iter().enumerate() {
        if run.borrow().names(n) {
            activated.push(id);
        }
    }
    let left: Vec<usize> = activated
        .iter()
        .copied()
        .filter(|id| !this_drain.iter().any(|(delivered, _)| delivered == id))
        .collect();
    if notified && !readable {
        failures.push(format!(
            "{cell}: the drain ended with the state notified and an empty trigger"
        ));
    }
    if !left.is_empty() && !readable {
        failures.push(format!(
            "{cell}: the drain left the activations {left:?} with no wake in the trigger"
        ));
    }

    notifier.notify(EventId::new(LATER)).unwrap();
    if !is_readable(&listener) {
        failures.push(format!(
            "{cell}: a notify after the drain left the trigger unreadable"
        ));
    }
    let mut next_drain = Vec::new();
    listener
        .try_wait(|activation| next_drain.push((activation.id.as_value(), activation.count)))
        .unwrap();
    let next_drain = sorted(next_drain);

    let mut every_activation = activated;
    every_activation.push(LATER);
    every_activation.sort_unstable();
    let both_drains = sorted(this_drain.iter().chain(&next_drain).copied().collect());
    if both_drains != once_each(&every_activation) {
        failures.push(format!(
            "{cell}: the two drains delivered {both_drains:?}, every activation once is {:?}",
            once_each(&every_activation)
        ));
    }

    let mut delivered = [0u8; 3];
    for (id, count) in &this_drain {
        let slot = match *id {
            PRIOR => 0,
            HOOKED => 1,
            SECOND => 2,
            _ => {
                failures.push(format!("{cell}: the drain delivered the unknown id {id}"));
                continue;
            }
        };
        delivered[slot] += u8::try_from(*count).unwrap();
    }
    let predicted = model::predict(start.model(), trigger, script);
    if (notified, readable, delivered)
        != (predicted.notified, predicted.units > 0, predicted.delivered)
    {
        failures.push(format!(
            "{cell}: the drain ended notified {notified}, readable {readable}, delivering \
             {this_drain:?}; the model of the order drain_events implements ends in {predicted:?}"
        ));
    }

    let end = ScriptEnd {
        this_drain,
        notified,
        readable,
        next_drain,
    };
    (end, failures)
}

/// The script that runs each notifier step of `steps` at its position, in the order of `steps`
/// within a position.
fn script_placing(steps: &[(usize, Actor)]) -> Vec<Actor> {
    let mut script = Vec::new();
    for position in 0..=LAST_POSITION {
        script.extend(
            steps
                .iter()
                .filter(|(at, _)| *at == position)
                .map(|(_, actor)| *actor),
        );
        if position < LAST_POSITION {
            script.push(D);
        }
    }
    script
}

/// Every placement of the four steps of one notify at the positions, in step order.
fn split_notify_placements() -> Vec<[usize; 4]> {
    let mut placements = Vec::new();
    for activate in 0..=LAST_POSITION {
        for claim in activate..=LAST_POSITION {
            for send in claim..=LAST_POSITION {
                for mark in send..=LAST_POSITION {
                    placements.push([activate, claim, send, mark]);
                }
            }
        }
    }
    placements
}

fn assert_no_failures(failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} failed checks:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

fn run_every_split_notify<E, Mgmt, Storage, H, W>(
    _event: PhantomData<EventImpl<E, Mgmt, Storage, H, W>>,
    trigger: TriggerRead,
) where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let placements = split_notify_placements();
    assert_eq!(placements.len(), 210);
    let mut cells_run = 0;
    let mut failures = Vec::new();
    for start in STARTS {
        for placement in &placements {
            let cell = format!("split notify ({start:?}, steps at {placement:?})");
            let script = script_placing(&placement.map(|at| (at, N0)));
            failures.extend(run_script::<E, Mgmt, Storage, H, W>(&cell, start, &script, trigger).1);
            cells_run += 1;
        }
    }
    assert_eq!(cells_run, 840);
    assert_no_failures(&failures);
}

fn run_every_notify_pair<E, Mgmt, Storage, H, W>(
    _event: PhantomData<EventImpl<E, Mgmt, Storage, H, W>>,
    trigger: TriggerRead,
) where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let mut cells_run = 0;
    let mut failures = Vec::new();
    for start in STARTS {
        for first in 0..=LAST_POSITION {
            for second in first..=LAST_POSITION {
                let cell = format!("notify pair ({start:?}, first at {first}, second at {second})");
                let mut steps = [(first, N0); 8];
                steps[4..].fill((second, N1));
                let script = script_placing(&steps);
                failures
                    .extend(run_script::<E, Mgmt, Storage, H, W>(&cell, start, &script, trigger).1);
                cells_run += 1;
            }
        }
    }
    assert_eq!(cells_run, 112);
    assert_no_failures(&failures);
}

/// The hand-written end of a model witness schedule on the real drain.
struct WitnessOracle {
    witness: &'static ModelWitness,
    /// The event ids the scripted drain delivers, ascending, each once.
    this_drain: &'static [usize],
    notified: bool,
    readable: bool,
    /// The event ids the drain after `notify(LATER)` delivers, ascending.
    next_drain: &'static [usize],
}

// The real drain reads the trigger before it re-arms and its load leaves a notified state
// as it is, so the witness steps land elsewhere than in the orders they were written for. From
// the idle start notifier 0 sends after the trigger read and keeps its unit, its mark fails on
// the re-armed state, and notifier 1 activates after the event state drain and claims the idle
// state. From the notified start notifier 0 skips on the notified state and notifier 1 claims
// the re-armed state before the event state drain. Each notify that sends leaves its unit, so
// every cell ends notified with a readable trigger.
const WITNESS_ORACLE: [WitnessOracle; 7] = [
    WitnessOracle {
        witness: &model::REARM_BEFORE_CONSUME_GOES_DEAF,
        this_drain: &[HOOKED],
        notified: true,
        readable: true,
        next_drain: &[LATER, SECOND],
    },
    WitnessOracle {
        witness: &model::CONSUME_BEFORE_REARM_KEEPS_THE_WAKE,
        this_drain: &[HOOKED],
        notified: true,
        readable: true,
        next_drain: &[LATER, SECOND],
    },
    WitnessOracle {
        witness: &model::REARM_AFTER_DRAINS_IF_NOTIFIED_GOES_DEAF,
        this_drain: &[HOOKED],
        notified: true,
        readable: true,
        next_drain: &[LATER, SECOND],
    },
    WitnessOracle {
        witness: &model::REARM_AFTER_DRAINS_IF_NOTIFIED_LOSES_A_WAKE,
        this_drain: &[HOOKED],
        notified: true,
        readable: true,
        next_drain: &[LATER, SECOND],
    },
    WitnessOracle {
        witness: &model::REARM_AFTER_DRAINS_LOSES_A_WAKE,
        this_drain: &[HOOKED],
        notified: true,
        readable: true,
        next_drain: &[LATER, SECOND],
    },
    WitnessOracle {
        witness: &model::THE_SWAP_GOES_DEAF_ON_A_READ_OF_EVERY_UNIT,
        this_drain: &[PRIOR, HOOKED, SECOND],
        notified: true,
        readable: true,
        next_drain: &[LATER],
    },
    WitnessOracle {
        witness: &model::THE_SWAP_KEEPS_A_UNIT_ON_A_ONE_UNIT_READ,
        this_drain: &[PRIOR, HOOKED, SECOND],
        notified: true,
        readable: true,
        next_drain: &[LATER],
    },
];

fn replay_every_model_witness<E, Mgmt, Storage, H, W>(
    _event: PhantomData<EventImpl<E, Mgmt, Storage, H, W>>,
    trigger: TriggerRead,
) where
    E: EventState + 'static,
    Mgmt: ZeroCopySend + Send + Sync + Debug + 'static,
    Storage: DynamicStorage<State<E, Mgmt>> + 'static,
    H: HandlerInterface<E, Mgmt, Storage> + 'static,
    W: WaiterInterface<E, Mgmt, Storage> + SynchronousMultiplexing,
{
    let oracle_names: Vec<&str> = WITNESS_ORACLE.iter().map(|row| row.witness.name).collect();
    let model_names: Vec<&str> = WITNESSES.iter().map(|witness| witness.name).collect();
    assert_eq!(oracle_names, model_names);

    let mut failures = Vec::new();
    for row in &WITNESS_ORACLE {
        let witness = row.witness;
        assert!(
            witness.waits_with_try() && !witness.notifiers_share_an_event_id(),
            "{}",
            witness.name
        );
        let cell = format!("model witness ({})", witness.name);
        let (end, checks) = run_script::<E, Mgmt, Storage, H, W>(
            &cell,
            Start::of_model(witness.start()),
            &witness.boundary_schedule(),
            trigger,
        );
        failures.extend(checks);
        let expected = ScriptEnd {
            this_drain: once_each(row.this_drain),
            notified: row.notified,
            readable: row.readable,
            next_drain: once_each(row.next_drain),
        };
        if end != expected {
            failures.push(format!(
                "{cell}: the drain ended in {end:?}, the oracle says {expected:?}"
            ));
        }
    }
    assert_no_failures(&failures);
}

#[test]
fn a_notify_split_across_the_steps_of_a_drain_leaves_a_unix_datagram_counting_listener_awake() {
    run_every_split_notify(
        PhantomData::<UnixDatagramShmCountingBitSet>,
        TriggerRead::OneUnit,
    );
}

#[test]
fn a_notify_split_across_the_steps_of_a_drain_leaves_a_unix_datagram_bitset_listener_awake() {
    run_every_split_notify(PhantomData::<UnixDatagramShmBitSet>, TriggerRead::OneUnit);
}

#[test]
fn a_notify_split_across_the_steps_of_a_drain_leaves_a_socket_pair_counting_listener_awake() {
    run_every_split_notify(
        PhantomData::<SocketPairCountingBitSet>,
        TriggerRead::EveryUnit,
    );
}

#[test]
fn two_notifies_at_any_steps_of_a_drain_leave_a_unix_datagram_counting_listener_awake() {
    run_every_notify_pair(
        PhantomData::<UnixDatagramShmCountingBitSet>,
        TriggerRead::OneUnit,
    );
}

#[test]
fn two_notifies_at_any_steps_of_a_drain_leave_a_unix_datagram_bitset_listener_awake() {
    run_every_notify_pair(PhantomData::<UnixDatagramShmBitSet>, TriggerRead::OneUnit);
}

#[test]
fn two_notifies_at_any_steps_of_a_drain_leave_a_socket_pair_counting_listener_awake() {
    run_every_notify_pair(
        PhantomData::<SocketPairCountingBitSet>,
        TriggerRead::EveryUnit,
    );
}

#[test]
fn the_model_witness_schedules_end_on_a_unix_datagram_counting_listener_as_the_oracle_says() {
    replay_every_model_witness(
        PhantomData::<UnixDatagramShmCountingBitSet>,
        TriggerRead::OneUnit,
    );
}

#[test]
fn the_model_witness_schedules_end_on_a_unix_datagram_bitset_listener_as_the_oracle_says() {
    replay_every_model_witness(PhantomData::<UnixDatagramShmBitSet>, TriggerRead::OneUnit);
}

#[test]
fn the_model_witness_schedules_end_on_a_socket_pair_counting_listener_as_the_oracle_says() {
    replay_every_model_witness(
        PhantomData::<SocketPairCountingBitSet>,
        TriggerRead::EveryUnit,
    );
}
