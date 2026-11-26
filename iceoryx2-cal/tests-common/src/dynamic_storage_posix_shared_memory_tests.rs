// Copyright (c) 2024 Contributors to the Eclipse Foundation
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

use core::time::Duration;
use iceoryx2_bb_elementary_traits::zero_copy_send::ZeroCopySend;
use iceoryx2_bb_posix::clock::Time;
use iceoryx2_bb_posix::creation_mode::CreationMode;
use iceoryx2_bb_posix::file::AccessMode;
use iceoryx2_bb_posix::permission::Permission;
use iceoryx2_bb_posix::shared_memory::SharedMemoryBuilder;
use iceoryx2_bb_posix::testing::generate_file_path;
use iceoryx2_bb_testing::assert_that;
use iceoryx2_bb_testing_macros::test;
use iceoryx2_cal::dynamic_storage::*;
use iceoryx2_cal::named_concept::*;
use iceoryx2_cal::testing::*;

const TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
#[repr(C)]
struct TestData {}

unsafe impl ZeroCopySend for TestData {}
unsafe impl Send for TestData {}
unsafe impl Sync for TestData {}

#[test]
fn version_check_works() {
    type Sut = iceoryx2_cal::dynamic_storage::posix_shared_memory::Storage<TestData>;
    let storage_name = generate_file_path().file_name();
    let config = generate_isolated_config::<Sut>();
    let file_name = config.path_for(&storage_name).file_name();

    let raw_shm = SharedMemoryBuilder::new(&file_name)
        .creation_mode(CreationMode::PurgeAndCreate)
        .size(1234)
        .has_ownership(true)
        .create()
        .unwrap();

    unsafe {
        *(raw_shm.base_address().as_ptr() as *mut u64) = u64::MAX;
    }

    let sut = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
        .config(&config)
        .open(AccessMode::ReadWrite);

    assert_that!(sut, is_err);
    assert_that!(sut.err().unwrap(), eq DynamicStorageOpenError::VersionMismatch);
}

#[test]
fn write_only_segment_is_not_initialized() {
    type Sut = iceoryx2_cal::dynamic_storage::posix_shared_memory::Storage<TestData>;
    let storage_name = generate_file_path().file_name();
    let config = generate_isolated_config::<Sut>();
    let file_name = config.path_for(&storage_name).file_name();

    let _raw_shm = SharedMemoryBuilder::new(&file_name)
        .creation_mode(CreationMode::PurgeAndCreate)
        .size(1234)
        .has_ownership(true)
        .permission(Permission::OWNER_WRITE)
        .create()
        .unwrap();

    let sut = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
        .config(&config)
        .open(AccessMode::ReadWrite);

    assert_that!(sut, is_err);
    assert_that!(sut.err().unwrap(), eq DynamicStorageOpenError::InitializationNotYetFinalized);
}

#[test]
fn waiting_for_initialization_works() {
    type Sut = iceoryx2_cal::dynamic_storage::posix_shared_memory::Storage<TestData>;
    let storage_name = generate_file_path().file_name();
    let config = generate_isolated_config::<Sut>();
    let file_name = config.path_for(&storage_name).file_name();

    let _raw_shm = SharedMemoryBuilder::new(&file_name)
        .creation_mode(CreationMode::PurgeAndCreate)
        .size(1234)
        .has_ownership(true)
        .permission(Permission::OWNER_WRITE)
        .create()
        .unwrap();

    let start = Time::now().expect("failed to get current time");
    let sut = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
        .timeout(TIMEOUT)
        .config(&config)
        .open(AccessMode::ReadWrite);

    assert_that!(sut, is_err);
    assert_that!(sut.err().unwrap(), eq DynamicStorageOpenError::InitializationNotYetFinalized);
    assert_that!(start.elapsed().expect("failed to get elapsed time"), ge TIMEOUT);
}

// macOS and FreeBSD emulate POSIX shared memory with a state file in the temp
// directory. A process that stops between writing that file and creating the
// shared memory leaves the name taken while nothing can be opened under it.
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
mod dangling_state_file {
    use super::{TIMEOUT, TestData};
    use core::time::Duration;
    use iceoryx2_bb_container::semantic_string::SemanticString;
    use iceoryx2_bb_posix::clock::Time;
    use iceoryx2_bb_posix::config::TEMP_DIRECTORY;
    use iceoryx2_bb_posix::creation_mode::CreationMode;
    use iceoryx2_bb_posix::file::AccessMode;
    use iceoryx2_bb_posix::file::{File, FileBuilder};
    use iceoryx2_bb_posix::permission::Permission;
    use iceoryx2_bb_posix::shared_memory::SharedMemory;
    use iceoryx2_bb_posix::testing::generate_file_path;
    use iceoryx2_bb_system_types::file_name::FileName;
    use iceoryx2_bb_system_types::file_path::FilePath;
    use iceoryx2_bb_testing::assert_that;
    use iceoryx2_bb_testing::watchdog::Watchdog;
    use iceoryx2_bb_testing_macros::test;
    use iceoryx2_cal::dynamic_storage::*;
    use iceoryx2_cal::named_concept::*;
    use iceoryx2_cal::testing::generate_isolated_config;

    type Sut = iceoryx2_cal::dynamic_storage::posix_shared_memory::Storage<TestData>;
    type SutConfig = <Sut as NamedConceptMgmt>::Configuration;

    const STATE_FILE_SUFFIX: &[u8] = b".shm_state";
    // The state file holds the real shared memory name in a 33 byte field.
    const REAL_NAME_FIELD_LEN: usize = 33;
    // Real names start with the creator's pid. No iceoryx2 process has pid 0,
    // so no shared memory with this real name exists.
    const ABSENT_REAL_NAME: &[u8] = b"0_0_0_0";
    // Slack for scheduling noise on top of the builder timeout.
    const RETURN_GRACE: Duration = Duration::from_secs(1);
    const EXPECTED_ERROR: DynamicStorageOpenOrCreateError =
        DynamicStorageOpenOrCreateError::DynamicStorageOpenError(
            DynamicStorageOpenError::InitializationNotYetFinalized,
        );

    fn state_file_path(storage_name: &FileName, config: &SutConfig) -> FilePath {
        let mut state_file_name = config.path_for(storage_name).file_name();
        state_file_name.push_bytes(STATE_FILE_SUFFIX).unwrap();
        FilePath::from_path_and_file(&TEMP_DIRECTORY, &state_file_name).unwrap()
    }

    /// Plants a state file and removes it again when dropped.
    fn plant_state_file(storage_name: &FileName, config: &SutConfig, content: &[u8]) -> File {
        let mut file = FileBuilder::new(&state_file_path(storage_name, config))
            .has_ownership(true)
            .creation_mode(CreationMode::CreateExclusive)
            .permission(Permission::OWNER_READ_WRITE)
            .create()
            .unwrap();
        file.write(content).unwrap();
        file
    }

    fn dangling_name_content() -> [u8; REAL_NAME_FIELD_LEN] {
        let mut content = [0u8; REAL_NAME_FIELD_LEN];
        content[..ABSENT_REAL_NAME.len()].copy_from_slice(ABSENT_REAL_NAME);
        content
    }

    fn open_or_create(
        storage_name: &FileName,
        config: &SutConfig,
        timeout: Duration,
    ) -> (Result<Sut, DynamicStorageOpenOrCreateError>, Duration) {
        let start = Time::now().expect("failed to get current time");
        let result = <Sut as DynamicStorage<TestData>>::Builder::new(storage_name)
            .config(config)
            .timeout(timeout)
            .initializer(|_, _| true)
            .open_or_create();
        (result, start.elapsed().expect("failed to get elapsed time"))
    }

    #[test]
    fn open_or_create_fails_after_the_timeout_when_the_state_file_names_absent_shared_memory() {
        let _watchdog = Watchdog::new();
        let storage_name = generate_file_path().file_name();
        let config = generate_isolated_config::<Sut>();
        let _state_file = plant_state_file(&storage_name, &config, &dangling_name_content());

        let (sut, elapsed) = open_or_create(&storage_name, &config, TIMEOUT);

        assert_that!(sut.err(), eq Some(EXPECTED_ERROR));
        assert_that!(elapsed, ge TIMEOUT);
        assert_that!(elapsed, lt TIMEOUT + RETURN_GRACE);
    }

    #[test]
    fn open_or_create_with_zero_timeout_fails_at_once_when_the_state_file_names_absent_shared_memory()
     {
        let _watchdog = Watchdog::new();
        let storage_name = generate_file_path().file_name();
        let config = generate_isolated_config::<Sut>();
        let _state_file = plant_state_file(&storage_name, &config, &dangling_name_content());

        let (sut, elapsed) = open_or_create(&storage_name, &config, Duration::ZERO);

        assert_that!(sut.err(), eq Some(EXPECTED_ERROR));
        assert_that!(elapsed, lt RETURN_GRACE);
    }

    #[test]
    fn open_or_create_fails_after_the_timeout_when_the_state_file_is_empty() {
        let _watchdog = Watchdog::new();
        let storage_name = generate_file_path().file_name();
        let config = generate_isolated_config::<Sut>();
        let _state_file = plant_state_file(&storage_name, &config, &[]);

        let (sut, elapsed) = open_or_create(&storage_name, &config, TIMEOUT);

        assert_that!(sut.err(), eq Some(EXPECTED_ERROR));
        assert_that!(elapsed, ge TIMEOUT);
        assert_that!(elapsed, lt TIMEOUT + RETURN_GRACE);
    }

    #[test]
    fn open_or_create_fails_the_same_way_on_every_attempt_against_a_dangling_state_file() {
        let _watchdog = Watchdog::new();
        let storage_name = generate_file_path().file_name();
        let config = generate_isolated_config::<Sut>();
        let _state_file = plant_state_file(&storage_name, &config, &dangling_name_content());

        for _ in 0..2 {
            let (sut, elapsed) = open_or_create(&storage_name, &config, TIMEOUT);

            assert_that!(sut.err(), eq Some(EXPECTED_ERROR));
            assert_that!(elapsed, ge TIMEOUT);
            assert_that!(elapsed, lt TIMEOUT + RETURN_GRACE);
        }
    }

    #[test]
    fn open_or_create_works_after_the_dangling_storage_is_removed() {
        let _watchdog = Watchdog::new();
        let storage_name = generate_file_path().file_name();
        let config = generate_isolated_config::<Sut>();
        let state_file = plant_state_file(&storage_name, &config, &dangling_name_content());
        state_file.release_ownership();

        // Nothing can be opened under the name, yet the name is taken.
        let sut = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .open(AccessMode::ReadWrite);
        assert_that!(sut.err(), eq Some(DynamicStorageOpenError::DoesNotExist));
        let sut = <Sut as DynamicStorage<TestData>>::Builder::new(&storage_name)
            .config(&config)
            .initializer(|_, _| true)
            .create();
        assert_that!(sut.err(), eq Some(DynamicStorageCreateError::AlreadyExists));

        let shm_name = config.path_for(&storage_name).file_name();
        assert_that!(SharedMemory::remove(&shm_name), is_ok);
        assert_that!(File::remove(&state_file_path(&storage_name, &config)), eq Ok(false));

        let (sut, _) = open_or_create(&storage_name, &config, TIMEOUT);

        assert_that!(sut, is_ok);
    }
}
