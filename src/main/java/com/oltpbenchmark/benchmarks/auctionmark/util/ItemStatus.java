/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.auctionmark.util;

/**
 * The local state of an item
 *
 * @author pavlo
 */
public enum ItemStatus {
    OPEN(false),
    ENDING_SOON(true), // Only used internally
    WAITING_FOR_PURCHASE(false),
    CLOSED(false);

    private final boolean internal;

    ItemStatus(boolean internal) {
        this.internal = internal;
    }

    public boolean isInternal() {
        return internal;
    }

    public static ItemStatus get(long idx) {
        return (values()[(int) idx]);
    }
}