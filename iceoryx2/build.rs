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

fn main() {
    // A crate that depends on this one reads `DEP_ICEORYX2_PUBLISHER_HISTORY`
    // in its build script: it is set when the publisher builder takes a
    // history size of its own (`PortFactoryPublisher::history_size`).
    println!("cargo:publisher_history=1");
    println!("cargo:rerun-if-changed=build.rs");
}
