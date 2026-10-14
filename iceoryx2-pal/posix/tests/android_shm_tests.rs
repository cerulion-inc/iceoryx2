#![cfg(target_os = "android")]

use std::ffi::{CStr, CString};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use iceoryx2_pal_posix::posix::{shm_list, shm_open, shm_unlink};

fn unique_name(label: &str) -> CString {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    CString::new(format!("/{label}_{}_{}", std::process::id(), nonce))
        .expect("test names contain no NUL bytes")
}

fn assert_errno(expected: i32) {
    assert_eq!(
        io::Error::last_os_error().raw_os_error(),
        Some(expected),
        "unexpected errno"
    );
}

fn list_contains(name: &[u8]) -> bool {
    // SAFETY: shm_list returns fixed-size names copied from NUL-terminated dirent entries.
    unsafe { shm_list() }.iter().any(|entry| {
        // SAFETY: each shm_list entry is NUL-terminated within its 256-byte buffer.
        unsafe { CStr::from_ptr(entry.as_ptr()).to_bytes() == name }
    })
}

#[test]
fn shared_file_descriptors_map_the_same_object_and_unlink_it() {
    const LENGTH: usize = 4096;
    const VALUE: u64 = 0x5a31_72c4_9be0_d8f6;

    let name = unique_name("shared");
    // SAFETY: `name` is a live NUL-terminated string for each call.
    let first_fd = unsafe {
        shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            0o600,
        )
    };
    assert!(
        first_fd >= 0,
        "create shared-memory object: {}",
        io::Error::last_os_error()
    );

    // SAFETY: `name` remains valid and identifies the object created above.
    let second_fd = unsafe { shm_open(name.as_ptr(), libc::O_RDWR, 0) };
    assert!(
        second_fd >= 0,
        "open second descriptor: {}",
        io::Error::last_os_error()
    );
    // SAFETY: `first_fd` is open and names the newly created object.
    assert_eq!(
        unsafe { libc::ftruncate(first_fd, LENGTH as libc::off_t) },
        0
    );

    // SAFETY: `first_fd` refers to a 4096-byte file; this is a shared writable mapping.
    let first_mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            LENGTH,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            first_fd,
            0,
        )
    };
    assert_ne!(first_mapping, libc::MAP_FAILED, "map first descriptor");

    // SAFETY: `second_fd` names the same 4096-byte file; this is a shared writable mapping.
    let second_mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            LENGTH,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            second_fd,
            0,
        )
    };
    assert_ne!(second_mapping, libc::MAP_FAILED, "map second descriptor");

    // SAFETY: the checked mappings are writable and each spans at least one u64.
    unsafe { std::ptr::write(first_mapping.cast::<u64>(), VALUE) };
    // SAFETY: the checked second mapping is readable and shares the same backing file.
    assert_eq!(
        unsafe { std::ptr::read(second_mapping.cast::<u64>()) },
        VALUE
    );
    assert!(
        list_contains(&name.to_bytes()[1..]),
        "shm_list includes the created name"
    );

    // SAFETY: each pointer is a live 4096-byte mapping returned by mmap.
    assert_eq!(unsafe { libc::munmap(first_mapping, LENGTH) }, 0);
    // SAFETY: each pointer is a live 4096-byte mapping returned by mmap.
    assert_eq!(unsafe { libc::munmap(second_mapping, LENGTH) }, 0);
    // SAFETY: both descriptors are open and are not used after closing.
    assert_eq!(unsafe { libc::close(first_fd) }, 0);
    // SAFETY: both descriptors are open and are not used after closing.
    assert_eq!(unsafe { libc::close(second_fd) }, 0);

    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, 0);
    assert!(
        !list_contains(&name.to_bytes()[1..]),
        "shm_list omits the name after unlink"
    );

    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_open(name.as_ptr(), libc::O_RDWR, 0) }, -1);
    assert_errno(libc::ENOENT);
}

#[test]
fn exclusive_create_and_missing_objects_return_posix_errors() {
    let name = unique_name("exclusive");
    // SAFETY: `name` is a live NUL-terminated string for each call.
    let fd = unsafe {
        shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            0o600,
        )
    };
    assert!(fd >= 0, "create shared-memory object");

    // SAFETY: `name` remains valid and identifies the existing object.
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
    assert_errno(libc::EEXIST);
    // SAFETY: `fd` is open and is not used after closing.
    assert_eq!(unsafe { libc::close(fd) }, 0);
    // SAFETY: `name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, 0);

    let missing = unique_name("missing");
    // SAFETY: `missing` is a live NUL-terminated string.
    assert_eq!(unsafe { shm_open(missing.as_ptr(), libc::O_RDWR, 0) }, -1);
    assert_errno(libc::ENOENT);
    // SAFETY: `missing` is a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(missing.as_ptr()) }, -1);
    assert_errno(libc::ENOENT);
}

#[test]
fn invalid_names_report_einval_and_enametoolong() {
    for value in ["", "missing_slash", "/", "/one/two", "/.", "/.."] {
        let name = CString::new(value).expect("test names contain no NUL bytes");
        // SAFETY: `name` is a live NUL-terminated string.
        assert_eq!(
            unsafe { shm_open(name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) },
            -1
        );
        assert_errno(libc::EINVAL);
        // SAFETY: `name` remains a live NUL-terminated string.
        assert_eq!(unsafe { shm_unlink(name.as_ptr()) }, -1);
        assert_errno(libc::EINVAL);
    }

    let long_name = CString::new(format!("/{}", "n".repeat(256))).expect("valid test name");
    // SAFETY: `long_name` is a live NUL-terminated string.
    assert_eq!(
        unsafe { shm_open(long_name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o600) },
        -1
    );
    assert_errno(libc::ENAMETOOLONG);
    // SAFETY: `long_name` remains a live NUL-terminated string.
    assert_eq!(unsafe { shm_unlink(long_name.as_ptr()) }, -1);
    assert_errno(libc::ENAMETOOLONG);
}
