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

//! Runs one complete notify at each step boundary of a real drain and checks that the
//! listener still wakes afterwards.
//!
//! Every cell is one start state crossed with one boundary. A listener and a notifier are
//! created over a real trigger, the start state is set up, and the drain hook runs
//! `notify(HOOKED)` once when the `try_wait()` drain reaches the cell's boundary. Each cell then
//! checks:
//!
//!  * the drain reached the boundary once when the oracle says it passes it, else never,
//!  * the drain delivered the activations of the cell's oracle row,
//!  * the drain did not end with the state notified and an empty trigger, the state in which
//!    every later notify skips its send,
//!  * an activation the drain left behind has a wake waiting in the trigger,
//!  * a later `notify(LATER)` makes the trigger readable, and the next drain delivers the
//!    activations of the oracle row.

use super::drain_gap::{self, DrainGap};
use super::{
    EventImpl, HandleBuilder, NOTIFICATION_STATE_IDLE, NOTIFICATION_STATE_NOTIFIED, WaiterBuilder,
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
use iceoryx2_bb_concurrency::cell::Cell;
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
    let state = || {
        listener
            .storage
            .get()
            .notification_state
            .load(Ordering::SeqCst)
    };

    match oracle.start {
        IdleEmpty => (),
        IdleStrayByte => notifier.handle.notify().unwrap(),
        NotifiedOneByte => notifier.notify(EventId::new(PRIOR)).unwrap(),
        NotifiedTwoBytes => {
            notifier.notify(EventId::new(PRIOR)).unwrap();
            notifier.handle.notify().unwrap();
        }
    }
    let start_state = match oracle.start {
        IdleEmpty | IdleStrayByte => NOTIFICATION_STATE_IDLE,
        NotifiedOneByte | NotifiedTwoBytes => NOTIFICATION_STATE_NOTIFIED,
    };
    assert_eq!(state(), start_state, "{cell}: start state");
    assert_eq!(
        is_readable(&listener),
        oracle.start != IdleEmpty,
        "{cell}: start trigger"
    );

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

    let notified_after = state() == NOTIFICATION_STATE_NOTIFIED;
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
