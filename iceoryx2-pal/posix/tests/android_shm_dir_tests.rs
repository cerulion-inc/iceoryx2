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
use std::time::{SystemTime, UNIX_EPOCH};

use iceoryx2_pal_posix::posix::{shm_open, shm_unlink};

#[test]
fn shm_open_uses_the_configured_directory() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "iceoryx2-android-shm-dir-{}-{nonce}",
        std::process::id()
    ));
    assert!(directory.is_absolute());
    // SAFETY: this is the binary's only test and sets the variable before shared-memory access.
    unsafe { std::env::set_var("CERULION_SHM_DIR", &directory) };

    let name = CString::new("/configured-directory").expect("valid object name");
    // SAFETY: `name` is a live NUL-terminated string.
    let fd = unsafe {
        shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            0o600,
        )
    };
    assert!(
        fd >= 0,
        "create shared-memory object: {}",
        std::io::Error::last_os_error()
    );
    assert!(
        directory.join("configured-directory").is_file(),
        "object file is created in CERULION_SHM_DIR"
    );

    // SAFETY: `fd` is open and is not used after closing.
    assert_eq!(unsafe { libc::close(fd) }, 0);
    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, 0);
    std::fs::remove_dir(&directory).expect("remove empty shared-memory directory");
}
