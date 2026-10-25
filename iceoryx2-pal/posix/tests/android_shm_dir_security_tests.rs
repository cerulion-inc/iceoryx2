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

#![cfg(target_os = "android")]

use std::ffi::CString;
use std::io;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use iceoryx2_pal_posix::posix::{shm_list, shm_open, shm_unlink};

const SENTINEL: &str = "sentinel-shm-object";

fn last_errno() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .expect("last OS error has an errno")
}

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .expect("set directory mode");
}

#[test]
fn unsafe_and_symlinked_directories_are_refused() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "iceoryx2-android-shm-dir-security-{}-{nonce}",
        std::process::id()
    ));
    let selected = root.join("selected");
    let target = root.join("target");
    std::fs::create_dir(&root).expect("create test root");

    // SAFETY: this is the binary's only test and sets the variable before shared-memory access.
    unsafe { std::env::set_var("CERULION_SHM_DIR", &selected) };

    let missing_name = CString::new("/missing-directory").expect("valid object name");
    // SAFETY: `missing_name` is a live NUL-terminated string.
    assert_eq!(
        unsafe { shm_open(missing_name.as_ptr(), libc::O_RDWR, 0) },
        -1
    );
    assert_eq!(last_errno(), libc::ENOENT);
    // SAFETY: `missing_name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(missing_name.as_ptr()) }, -1);
    assert_eq!(last_errno(), libc::ENOENT);
    assert!(
        !selected.exists(),
        "non-create operations do not create the directory"
    );

    std::fs::create_dir(&selected).expect("create selected directory");
    // A sentinel entry makes the empty `shm_list()` below prove that the
    // directory was refused, not merely that it was empty.
    std::fs::write(selected.join(SENTINEL), b"").expect("create sentinel in selected directory");
    set_mode(&selected, 0o777);

    let name = CString::new("/unsafe-directory").expect("valid object name");
    // SAFETY: `name` is a live NUL-terminated string.
    assert_eq!(
        unsafe {
            shm_open(
                name.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        },
        -1
    );
    assert_eq!(last_errno(), libc::EACCES);
    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, -1);
    assert_eq!(last_errno(), libc::EACCES);
    // SAFETY: shm_list reads the configured directory and returns owned name buffers.
    assert!(unsafe { shm_list() }.is_empty());

    std::fs::remove_file(selected.join(SENTINEL)).expect("remove sentinel from selected directory");
    std::fs::remove_dir(&selected).expect("remove unsafe directory");
    std::fs::create_dir(&target).expect("create symlink target");
    // A sentinel entry makes the empty `shm_list()` below prove that the
    // symlink was not followed, not merely that its target was empty.
    std::fs::write(target.join(SENTINEL), b"").expect("create sentinel in symlink target");
    set_mode(&target, 0o700);
    symlink(&target, &selected).expect("replace selected directory with symlink");

    let symlink_name = CString::new("/symlink-directory").expect("valid object name");
    // SAFETY: `symlink_name` is a live NUL-terminated string.
    assert_eq!(
        unsafe {
            shm_open(
                symlink_name.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                0o600,
            )
        },
        -1
    );
    let open_errno = last_errno();
    assert_eq!(open_errno, libc::ENOTDIR);
    // SAFETY: `symlink_name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(symlink_name.as_ptr()) }, -1);
    assert_eq!(last_errno(), open_errno);
    // SAFETY: shm_list reads the configured directory and returns owned name buffers.
    assert!(unsafe { shm_list() }.is_empty());

    std::fs::remove_file(&selected).expect("remove directory symlink");
    std::fs::remove_file(target.join(SENTINEL)).expect("remove sentinel from symlink target");
    std::fs::remove_dir(&target).expect("remove symlink target");
    std::fs::remove_dir(&root).expect("remove test root");
}
