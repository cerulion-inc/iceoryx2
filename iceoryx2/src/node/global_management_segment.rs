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

extern crate alloc;

use crate::{config::Config, service::Service};
use alloc::format;
use core::mem::MaybeUninit;

use iceoryx2_bb_concurrency::atomic::{AtomicU32, Ordering};
use iceoryx2_bb_container::semantic_string::SemanticString;
use iceoryx2_bb_derive_macros::ZeroCopySend;
use iceoryx2_bb_elementary::package_version::PackageVersion;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_bb_system_types::file_name::FileName;
use iceoryx2_cal::dynamic_storage::*;
use iceoryx2_cal::named_concept::*;
use iceoryx2_log::fail;
use iceoryx2_log::fatal_panic;

const GLOBAL_MGMT_NAME: FileName = unsafe { FileName::new_unchecked_const(b"node") };

#[derive(Debug, Default, ZeroCopySend)]
#[repr(C)]
struct State {
    node_counter: AtomicU32,
}

impl State {
    fn initialize(state: &mut MaybeUninit<Self>) {
        state.write(Self::default());
    }
}

#[derive(Debug)]
pub(crate) struct GlobalManagementSegment<S: Service> {
    storage: S::PersistentDynamicStorage<State>,
}

impl<S: Service> GlobalManagementSegment<S> {
    pub fn open_or_create(global_config: &Config) -> Result<Self, DynamicStorageOpenOrCreateError> {
        let origin = "GlobalManagementSegment::open_or_create()";
        let msg = "Unable to open or create the management segment";
        let config = Self::dynamic_storage_config(global_config);
        let storage = match <<S::PersistentDynamicStorage<State> as DynamicStorage<State>>::Builder<
            '_,
        > as NamedConceptBuilder<S::PersistentDynamicStorage<State>>>::new(
            &GLOBAL_MGMT_NAME
        )
        .has_ownership(false)
        .config(&config)
        .timeout(global_config.global.creation_timeout)
        .enable_global_access(true)
        .initializer(|state, _| {
            State::initialize(state);
            true
        })
        .open_or_create()
        {
            Ok(storage) => storage,
            Err(e) => {
                fail!(from origin, with e,
                    "{msg} since the underlying persistent dynamic storage could not be opened. [{e:?}]");
            }
        };

        Ok(Self { storage })
    }

    /// # Safety
    ///
    /// * It must be ensured that !NO! other process is running currently using the
    ///   same iceoryx2 domain.
    ///
    pub(crate) unsafe fn remove(global_config: &Config) -> Result<bool, NamedConceptRemoveError> {
        let origin = "GlobalManagementSegment::remove()";
        let msg = "Unable to remove the global management segment";
        let config = Self::dynamic_storage_config(global_config);
        match unsafe {
            <S::PersistentDynamicStorage<State> as NamedConceptMgmt>::remove_cfg(
                &GLOBAL_MGMT_NAME,
                &config,
            )
        } {
            Ok(v) => Ok(v),
            Err(e) => {
                fail!(from origin, with e, "{msg} due to {e:?}.");
            }
        }
    }

    pub fn increment_node_counter(&self) -> u32 {
        self.storage
            .get()
            .node_counter
            .fetch_add(1, Ordering::Relaxed)
    }

    fn dynamic_storage_config(
        global_config: &Config,
    ) -> <S::PersistentDynamicStorage<State> as NamedConceptMgmt>::Configuration {
        let mut suffix = global_config.global.node.global_mgmt_suffix;
        let ver = PackageVersion::get();
        fatal_panic!(
            from "dynamic_storage_config()",
            when suffix.insert_bytes(0, format!(".{}_{}_{}", ver.major(), ver.minor(), ver.patch()).as_bytes()),
            "This should never happen! Failed to added package version suffix to global management segment.");

        <S::PersistentDynamicStorage<State> as NamedConceptMgmt>::Configuration::default()
            .prefix(&global_config.global.prefix)
            .suffix(&suffix)
            .path_hint(global_config.global.root_path())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::fmt::Debug;
    use core::ptr::NonNull;
    use core::time::Duration;
    use iceoryx2_bb_elementary::bump_allocator::BumpAllocator;
    use iceoryx2_bb_elementary_traits::testing::abandonable::Abandonable;
    use iceoryx2_bb_posix::access_mode::AccessMode;
    use iceoryx2_bb_system_types::path::Path;
    use iceoryx2_cal::dynamic_storage::process_local;
    use iceoryx2_cal::shm_allocator::pool_allocator::PoolAllocator;
    use iceoryx2_cal::*;

    const PREFILL_BYTE: u8 = 0xa5;

    /// A process-local dynamic storage that fills the memory of a newly created storage with
    /// [`PREFILL_BYTE`] before it calls the user initializer. Real shared memory is zero-filled
    /// by the kernel, so this is the only way to make a missing initialization observable.
    #[derive(Debug)]
    struct PrefilledStorage<T: Send + Sync + Debug + ZeroCopySend + 'static>(
        process_local::Storage<T>,
    );

    #[derive(Debug)]
    struct PrefilledBuilder<'builder, T: Send + Sync + Debug + ZeroCopySend + 'static>(
        process_local::Builder<'builder, T>,
    );

    impl<T: Send + Sync + Debug + ZeroCopySend + 'static> NamedConcept for PrefilledStorage<T> {
        fn name(&self) -> &FileName {
            self.0.name()
        }
    }

    impl<T: Send + Sync + Debug + ZeroCopySend + 'static> NamedConceptMgmt for PrefilledStorage<T> {
        type Configuration = <process_local::Storage<T> as NamedConceptMgmt>::Configuration;

        unsafe fn remove_cfg(
            name: &FileName,
            cfg: &Self::Configuration,
        ) -> Result<bool, NamedConceptRemoveError> {
            // SAFETY: the caller upholds the contract of `remove_cfg`, which is forwarded as is.
            unsafe { process_local::Storage::<T>::remove_cfg(name, cfg) }
        }

        fn does_exist_cfg(
            name: &FileName,
            cfg: &Self::Configuration,
        ) -> Result<bool, NamedConceptDoesExistError> {
            process_local::Storage::<T>::does_exist_cfg(name, cfg)
        }

        fn list_cfg(cfg: &Self::Configuration) -> Result<Vec<FileName>, NamedConceptListError> {
            process_local::Storage::<T>::list_cfg(cfg)
        }

        fn remove_path_hint(value: &Path) -> Result<(), NamedConceptPathHintRemoveError> {
            process_local::Storage::<T>::remove_path_hint(value)
        }
    }

    impl<T: Send + Sync + Debug + ZeroCopySend + 'static> Abandonable for PrefilledStorage<T> {
        unsafe fn abandon_in_place(mut this: NonNull<Self>) {
            // SAFETY: `this` is valid by the contract of this function and the wrapper has no
            // other field, so abandoning the inner storage in place abandons all of `Self`.
            unsafe {
                process_local::Storage::<T>::abandon_in_place(NonNull::from_mut(
                    &mut this.as_mut().0,
                ))
            }
        }
    }

    impl<T: Send + Sync + Debug + ZeroCopySend + 'static> DynamicStorage<T> for PrefilledStorage<T> {
        type Builder<'builder> = PrefilledBuilder<'builder, T>;

        fn does_support_persistency() -> bool {
            process_local::Storage::<T>::does_support_persistency()
        }

        fn has_ownership(&self) -> bool {
            self.0.has_ownership()
        }

        fn release_ownership(&self) {
            self.0.release_ownership()
        }

        fn acquire_ownership(&self) {
            self.0.acquire_ownership()
        }

        fn get(&self) -> &T {
            self.0.get()
        }

        unsafe fn get_mut(&self) -> &mut T {
            // SAFETY: the caller upholds the contract of `get_mut`, which is forwarded as is.
            unsafe { self.0.get_mut() }
        }

        unsafe fn __internal_set_type_name_in_config(
            config: &mut Self::Configuration,
            type_name: &str,
        ) {
            // SAFETY: the caller upholds the contract of the inner storage, which is forwarded
            // as is.
            unsafe {
                process_local::Storage::<T>::__internal_set_type_name_in_config(config, type_name)
            }
        }
    }

    impl<T: Send + Sync + Debug + ZeroCopySend + 'static> NamedConceptBuilder<PrefilledStorage<T>>
        for PrefilledBuilder<'_, T>
    {
        fn new(name: &FileName) -> Self {
            Self(process_local::Builder::new(name))
        }

        fn config(self, config: &<PrefilledStorage<T> as NamedConceptMgmt>::Configuration) -> Self {
            Self(self.0.config(config))
        }
    }

    impl<'builder, T: Send + Sync + Debug + ZeroCopySend + 'static>
        DynamicStorageBuilder<'builder, T, PrefilledStorage<T>> for PrefilledBuilder<'builder, T>
    {
        fn has_ownership(self, value: bool) -> Self {
            Self(self.0.has_ownership(value))
        }

        fn enable_global_access(self, value: bool) -> Self {
            Self(self.0.enable_global_access(value))
        }

        fn supplementary_size(self, value: usize) -> Self {
            Self(self.0.supplementary_size(value))
        }

        fn timeout(self, value: Duration) -> Self {
            Self(self.0.timeout(value))
        }

        fn initializer<F: FnOnce(&mut MaybeUninit<T>, &mut BumpAllocator) -> bool + 'builder>(
            self,
            value: F,
        ) -> Self {
            Self(self.0.initializer(move |state, allocator| {
                // SAFETY: `state` points to `size_of::<T>()` writable bytes. They stay
                // uninitialized for `T` until `value` initializes them.
                unsafe {
                    state
                        .as_mut_ptr()
                        .cast::<u8>()
                        .write_bytes(PREFILL_BYTE, core::mem::size_of::<T>())
                };
                value(state, allocator)
            }))
        }

        fn create(self) -> Result<PrefilledStorage<T>, DynamicStorageCreateError> {
            self.0.create().map(PrefilledStorage)
        }

        fn open(
            self,
            access_mode: AccessMode,
        ) -> Result<PrefilledStorage<T>, DynamicStorageOpenError> {
            self.0.open(access_mode).map(PrefilledStorage)
        }

        fn open_or_create(self) -> Result<PrefilledStorage<T>, DynamicStorageOpenOrCreateError> {
            self.0.open_or_create().map(PrefilledStorage)
        }
    }

    /// [`crate::service::local::Service`] with a [`PrefilledStorage`] as persistent dynamic
    /// storage.
    #[derive(Debug, Clone)]
    struct PrefilledService {}

    impl Service for PrefilledService {
        type StaticStorage = static_storage::recommended::Local;
        type ConfigSerializer = serialize::recommended::Recommended;
        type PersistentDynamicStorage<T: Debug + Send + Sync + ZeroCopySend + 'static> =
            PrefilledStorage<T>;
        type Bag = bag::recommended::Recommended;
        type DynamicStorage<T: Debug + Send + Sync + ZeroCopySend + 'static> =
            dynamic_storage::recommended::Local<T>;
        type ServiceNameHasher = hash::recommended::Recommended;
        type SharedMemory = shared_memory::recommended::Local<PoolAllocator>;
        type ResizableSharedMemory = resizable_shared_memory::recommended::Local<PoolAllocator>;
        type Connection = zero_copy_connection::recommended::Local;
        type Event = event::recommended::Local;
        type Monitoring = monitoring::recommended::Local;
        type Reactor = reactor::recommended::Local;
        type ArcThreadSafetyPolicy<T: Send + Debug + Abandonable> =
            arc_sync_policy::single_threaded::SingleThreaded<T>;
        type BlackboardMgmt<KeyType: Send + Sync + Debug + ZeroCopySend + 'static> =
            dynamic_storage::recommended::Local<KeyType>;
        type BlackboardPayload = shared_memory::recommended::Local<
            iceoryx2_cal::shm_allocator::bump_allocator::BumpAllocator,
        >;
        type UniqueId =
            crate::unique_id_generator::unique_system_id::UniqueSystemIdGenerator<PrefilledService>;
        type NameGenerator = crate::name_generator::default_name_generator::DefaultNameGenerator;
        type Clock = clock::posix::PosixClock<Self::Event>;
    }

    impl crate::service::internal::ServiceInternal<PrefilledService> for PrefilledService {}

    #[test]
    fn open_or_create_starts_node_counter_at_zero_when_new_storage_contains_other_bytes() {
        let config = crate::testing::generate_isolated_config();

        let sut = GlobalManagementSegment::<PrefilledService>::open_or_create(&config).unwrap();

        // Hand-written oracle: a fresh segment starts at 0, then 1. If open_or_create stops
        // writing the state, the counter reads the prefill pattern 0xa5a5a5a5 = 2779096485.
        assert_eq!(sut.increment_node_counter(), 0);
        assert_eq!(sut.increment_node_counter(), 1);

        drop(sut);
        // SAFETY: the config is isolated, so no other process or test uses this segment.
        assert!(unsafe { GlobalManagementSegment::<PrefilledService>::remove(&config) }.unwrap());
    }

    #[test]
    fn prefilled_storage_hands_the_initializer_non_zero_memory() {
        let config = crate::testing::generate_isolated_config();
        let storage_config =
            GlobalManagementSegment::<PrefilledService>::dynamic_storage_config(&config);
        let mut observed = 0u32;

        let sut =
            <PrefilledBuilder<'_, State> as NamedConceptBuilder<PrefilledStorage<State>>>::new(
                &GLOBAL_MGMT_NAME,
            )
            .has_ownership(false)
            .config(&storage_config)
            .initializer(|state, _| {
                // SAFETY: `State` is repr(C) with one AtomicU32, so the pointer is u32 aligned,
                // and the builder prefilled all 4 bytes, so the read is in bounds and initialized.
                observed = unsafe { state.as_ptr().cast::<u32>().read() };
                State::initialize(state);
                true
            })
            .create()
            .unwrap();
        drop(sut);

        assert_eq!(observed, 0xa5a5_a5a5);
        // SAFETY: the config is isolated, so no other process or test uses this segment.
        assert!(unsafe { GlobalManagementSegment::<PrefilledService>::remove(&config) }.unwrap());
    }

    #[test]
    fn initialize_sets_node_counter_to_zero_when_memory_contains_other_bytes() {
        let mut state = MaybeUninit::<State>::uninit();
        unsafe {
            state
                .as_mut_ptr()
                .cast::<u8>()
                .write_bytes(0xa5, core::mem::size_of::<State>())
        };

        State::initialize(&mut state);

        let state = unsafe { state.assume_init_ref() };
        assert_eq!(state.node_counter.load(Ordering::Relaxed), 0);
    }
}
