// Copyright (c) 2023 Contributors to the Eclipse Foundation
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

#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use crate::posix::{Errno, closedir, opendir, readdir, types::*};
use alloc::ffi::CString;
use core::ffi::CStr;
use std::ffi::OsStr;
use std::io::{self, ErrorKind};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

enum ShmError {
    Validation(Errno),
    Io(io::Error),
    Libc,
}

fn resolved_shm_dir() -> &'static PathBuf {
    static SHM_DIR: OnceLock<PathBuf> = OnceLock::new();

    SHM_DIR.get_or_init(|| {
        if let Some(path) = std::env::var_os("CERULION_SHM_DIR").map(PathBuf::from)
            && !path.as_os_str().is_empty()
            && path.is_absolute()
        {
            return path;
        }

        if let Some(path) = std::env::var_os("TMPDIR").map(PathBuf::from)
            && path.is_absolute()
        {
            return path.join("cerulion-shm");
        }

        let shared_memory_directory = iceoryx2_pal_configuration::SHARED_MEMORY_DIRECTORY
            .strip_suffix(b"/")
            .unwrap_or(iceoryx2_pal_configuration::SHARED_MEMORY_DIRECTORY);
        PathBuf::from(OsStr::from_bytes(shared_memory_directory))
    })
}

fn object_path(name: &CStr) -> Result<PathBuf, Errno> {
    let name = name.to_bytes();
    if name.len() < 2 || name[0] != b'/' {
        return Err(Errno::EINVAL);
    }

    let object_name = &name[1..];
    if object_name.len() > 255 {
        return Err(Errno::ENAMETOOLONG);
    }
    if object_name.contains(&b'/') || object_name == b"." || object_name == b".." {
        return Err(Errno::EINVAL);
    }

    Ok(resolved_shm_dir().join(OsStr::from_bytes(object_name)))
}

fn create_shm_dir(path: &Path) -> io::Result<()> {
    let create_dir = || {
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder.create(path)
    };

    match create_dir() {
        Ok(()) => std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            match create_dir() {
                Ok(()) => std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
                Err(error) => Err(error),
            }
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn io_error_errno(error: io::Error) -> Errno {
    let errno: Errno = error.raw_os_error().unwrap_or(libc::EIO).into();
    if errno == Errno::NOTIMPLEMENTED {
        Errno::EIO
    } else {
        errno
    }
}

fn open_in(name: &CStr, oflag: int, mode: mode_t) -> Result<int, ShmError> {
    let path = object_path(name).map_err(ShmError::Validation)?;
    if oflag & libc::O_CREAT != 0 {
        create_shm_dir(resolved_shm_dir()).map_err(ShmError::Io)?;
    }

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| ShmError::Validation(Errno::EINVAL))?;
    // SAFETY: `path` is a NUL-terminated path built from the validated name.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            oflag | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 { Err(ShmError::Libc) } else { Ok(fd) }
}

fn unlink_in(name: &CStr) -> Result<(), ShmError> {
    let path = object_path(name).map_err(ShmError::Validation)?;
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| ShmError::Validation(Errno::EINVAL))?;
    // SAFETY: `path` is a NUL-terminated path built from the validated name.
    if unsafe { libc::unlink(path.as_ptr()) } == 0 {
        Ok(())
    } else {
        Err(ShmError::Libc)
    }
}

pub unsafe fn mlock(addr: *const void, len: size_t) -> int {
    unsafe { libc::mlock(addr, len) }
}

pub unsafe fn munlock(addr: *const void, len: size_t) -> int {
    unsafe { libc::munlock(addr, len) }
}

pub unsafe fn mlockall(flags: int) -> int {
    unsafe { libc::mlockall(flags) }
}

pub unsafe fn munlockall() -> int {
    unsafe { libc::munlockall() }
}

pub unsafe fn shm_open(name: *const c_char, oflag: int, mode: mode_t) -> int {
    // SAFETY: the caller must provide a valid NUL-terminated name.
    match open_in(unsafe { CStr::from_ptr(name) }, oflag, mode) {
        Ok(fd) => fd,
        Err(ShmError::Validation(errno)) => {
            Errno::set(errno);
            -1
        }
        Err(ShmError::Io(error)) => {
            Errno::set(io_error_errno(error));
            -1
        }
        Err(ShmError::Libc) => -1,
    }
}

pub unsafe fn shm_unlink(name: *const c_char) -> int {
    // SAFETY: the caller must provide a valid NUL-terminated name.
    match unlink_in(unsafe { CStr::from_ptr(name) }) {
        Ok(()) => 0,
        Err(ShmError::Validation(errno)) => {
            Errno::set(errno);
            -1
        }
        Err(ShmError::Io(error)) => {
            Errno::set(io_error_errno(error));
            -1
        }
        Err(ShmError::Libc) => -1,
    }
}

pub unsafe fn shm_list() -> Vec<[i8; 256]> {
    let mut result = vec![];
    let Ok(path) = CString::new(resolved_shm_dir().as_os_str().as_bytes()) else {
        return result;
    };
    // SAFETY: `path` is a live NUL-terminated directory path.
    let dir = unsafe { opendir(path.as_ptr()) };
    if dir.is_null() {
        return result;
    }

    loop {
        // SAFETY: `dir` was returned by opendir and remains open until the loop ends.
        let entry = unsafe { readdir(dir) };
        if entry.is_null() {
            break;
        }

        // SAFETY: readdir returned an entry whose d_name is NUL-terminated.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        let name = name.to_bytes();
        if name.is_empty() || name == b"." || name == b".." {
            continue;
        }

        let mut temp = [0i8; 256];
        for (destination, source) in temp.iter_mut().zip(name) {
            *destination = *source as i8;
        }
        result.push(temp);
    }

    // SAFETY: `dir` was returned by opendir and has not been closed.
    unsafe { closedir(dir) };
    result
}

pub unsafe fn mmap(
    addr: *mut void,
    len: size_t,
    prot: int,
    flags: int,
    fd: int,
    off: off_t,
) -> *mut void {
    unsafe { libc::mmap(addr, len, prot, flags, fd, off) }
}

pub unsafe fn munmap(addr: *mut void, len: size_t) -> int {
    unsafe { libc::munmap(addr, len) }
}

pub unsafe fn mprotect(addr: *mut void, len: size_t, prot: int) -> int {
    unsafe { libc::mprotect(addr, len, prot) }
}
