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

use crate::posix::{Errno, closedir, readdir, types::*};
use alloc::ffi::CString;
use core::ffi::CStr;
use std::ffi::OsStr;
use std::io::{self, ErrorKind};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

enum ShmError {
    Validation(Errno),
    Errno(Errno),
    Io(io::Error),
    Libc,
}

fn configured_shm_dir() -> PathBuf {
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
}

/// Drops `.` components and trailing separators so the final component is the
/// directory itself, which `O_NOFOLLOW` then refuses when it is a symlink.
/// A path containing `..` is refused, since its final component would resolve
/// through whatever directory precedes it.
fn normalized_shm_dir(path: &Path) -> Result<PathBuf, Errno> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(Errno::EINVAL);
    }
    Ok(path.components().collect())
}

fn resolved_shm_dir() -> Result<&'static Path, ShmError> {
    static SHM_DIR: OnceLock<Result<PathBuf, Errno>> = OnceLock::new();

    match SHM_DIR.get_or_init(|| normalized_shm_dir(&configured_shm_dir())) {
        Ok(path) => Ok(path),
        Err(errno) => Err(ShmError::Validation(*errno)),
    }
}

fn object_name(name: &CStr) -> Result<&[u8], Errno> {
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

    Ok(object_name)
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

fn open_shm_dir(path: &Path) -> Result<OwnedFd, ShmError> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| ShmError::Validation(Errno::EINVAL))?;
    // SAFETY: `path` is a live NUL-terminated directory path for the duration of the call.
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(ShmError::Libc);
    }

    // SAFETY: `fd` is an open descriptor whose ownership moves into `dir_fd`.
    let dir_fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut status = core::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `status` points to writable storage for `libc::stat`, and `dir_fd` is open.
    if unsafe { libc::fstat(dir_fd.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(ShmError::Libc);
    }
    // SAFETY: `fstat` initialized the complete structure on success.
    let status = unsafe { status.assume_init() };
    // SAFETY: `geteuid` takes no arguments and has no memory-safety preconditions.
    let effective_uid = unsafe { libc::geteuid() };
    if status.st_uid != effective_uid || status.st_mode & 0o022 != 0 {
        return Err(ShmError::Errno(Errno::EACCES));
    }

    Ok(dir_fd)
}

fn open_in(name: &CStr, oflag: int, mode: mode_t) -> Result<int, ShmError> {
    let object_name = object_name(name).map_err(ShmError::Validation)?;
    let shm_dir = resolved_shm_dir()?;
    if oflag & libc::O_CREAT != 0 {
        create_shm_dir(shm_dir).map_err(ShmError::Io)?;
    }

    let dir_fd = open_shm_dir(shm_dir)?;
    let object_name = CString::new(object_name).map_err(|_| ShmError::Validation(Errno::EINVAL))?;
    // SAFETY: `dir_fd` is an open directory descriptor and `object_name` is a
    // NUL-terminated validated name containing no '/'.
    let fd = unsafe {
        libc::openat(
            dir_fd.as_raw_fd(),
            object_name.as_ptr(),
            oflag | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            mode,
        )
    };
    if fd < 0 { Err(ShmError::Libc) } else { Ok(fd) }
}

fn unlink_in(name: &CStr) -> Result<(), ShmError> {
    let object_name = object_name(name).map_err(ShmError::Validation)?;
    let dir_fd = open_shm_dir(resolved_shm_dir()?)?;
    let object_name = CString::new(object_name).map_err(|_| ShmError::Validation(Errno::EINVAL))?;
    // SAFETY: `dir_fd` is an open directory descriptor and `object_name` is a
    // NUL-terminated validated name containing no '/'.
    if unsafe { libc::unlinkat(dir_fd.as_raw_fd(), object_name.as_ptr(), 0) } == 0 {
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
        Err(ShmError::Errno(errno)) => {
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
        Err(ShmError::Errno(errno)) => {
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
    let Ok(dir_fd) = resolved_shm_dir().and_then(open_shm_dir) else {
        return result;
    };
    let fd = dir_fd.into_raw_fd();
    // SAFETY: `fd` is an open directory descriptor; on success, `fdopendir`
    // takes ownership and `closedir` below releases it.
    let dir = unsafe { libc::fdopendir(fd) };
    if dir.is_null() {
        // SAFETY: POSIX specifies that `fdopendir` leaves `fd` open on failure.
        unsafe { libc::close(fd) };
        return result;
    }

    loop {
        // SAFETY: `dir` was returned by `fdopendir` and remains open until the loop ends.
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

    // SAFETY: `dir` was returned by `fdopendir` and has not been closed.
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
