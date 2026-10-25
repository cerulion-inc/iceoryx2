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
use std::time::{SystemTime, UNIX_EPOCH};

use iceoryx2_pal_posix::posix::{shm_list, shm_open, shm_unlink};

fn last_errno() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .expect("last OS error has an errno")
}

#[test]
fn a_trailing_dot_does_not_let_a_symlinked_directory_through() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "iceoryx2-android-shm-dir-dot-{}-{nonce}",
        std::process::id()
    ));
    let target = root.join("target");
    let link = root.join("link");
    std::fs::create_dir(&root).expect("create test root");
    std::fs::create_dir(&target).expect("create symlink target");
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700))
        .expect("set target mode");
    symlink(&target, &link).expect("create directory symlink");

    // SAFETY: this is the binary's only test and sets the variable before shared-memory access.
    unsafe { std::env::set_var("CERULION_SHM_DIR", link.join(".")) };

    let name = CString::new("/dot-suffix").expect("valid object name");
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
    assert_eq!(last_errno(), libc::ENOTDIR);
    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, -1);
    assert_eq!(last_errno(), libc::ENOTDIR);
    // SAFETY: shm_list reads the configured directory and returns owned name buffers.
    assert!(unsafe { shm_list() }.is_empty());
    assert!(
        std::fs::read_dir(&target)
            .expect("read symlink target")
            .next()
            .is_none(),
        "no object is created through the symlink"
    );

    std::fs::remove_file(&link).expect("remove directory symlink");
    std::fs::remove_dir(&target).expect("remove symlink target");
    std::fs::remove_dir(&root).expect("remove test root");
}
