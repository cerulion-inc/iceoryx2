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
use std::time::{SystemTime, UNIX_EPOCH};

use iceoryx2_pal_posix::posix::{shm_list, shm_open, shm_unlink};

const SENTINEL: &str = "sentinel-shm-object";

fn last_errno() -> i32 {
    io::Error::last_os_error()
        .raw_os_error()
        .expect("last OS error has an errno")
}

#[test]
fn a_directory_with_a_parent_component_is_refused() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "iceoryx2-android-shm-dir-parent-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&root).expect("create test root");
    // `selected/..` names `root`. A sentinel entry there makes the empty
    // `shm_list()` below prove that the directory was refused, not merely
    // that it was empty.
    std::fs::write(root.join(SENTINEL), b"").expect("create sentinel in test root");

    // SAFETY: this is the binary's only test and sets the variable before shared-memory access.
    unsafe { std::env::set_var("CERULION_SHM_DIR", root.join("selected").join("..")) };

    let name = CString::new("/parent-component").expect("valid object name");
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
    assert_eq!(last_errno(), libc::EINVAL);
    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, -1);
    assert_eq!(last_errno(), libc::EINVAL);
    // SAFETY: shm_list reads the configured directory and returns owned name buffers.
    assert!(unsafe { shm_list() }.is_empty());
    let entries: Vec<_> = std::fs::read_dir(&root)
        .expect("read test root")
        .map(|entry| entry.expect("read test root entry").file_name())
        .collect();
    assert_eq!(
        entries,
        [SENTINEL],
        "nothing is created for a refused directory"
    );

    std::fs::remove_file(root.join(SENTINEL)).expect("remove sentinel from test root");
    std::fs::remove_dir(&root).expect("remove test root");
}
