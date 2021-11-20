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

package com.oltpbenchmark.types;

public enum TransactionStatus {
    /**
     * Unknown status
     */
    UNKNOWN,
    /**
     * The transaction executed successfully and
     * committed without any errors.
     */
    SUCCESS,
    /**
     * The transaction executed successfully but then was aborted
     * due to the valid user control code.
     * This is not an error.
     */
    USER_ABORTED,
    /**
     * The transaction did not execute due to internal
     * benchmark state. It should be retried
     */
    RETRY,

    /**
     * The transaction did not execute due to internal
     * benchmark state. The Worker should retry but select
     * a new random transaction to execute.
     */
    RETRY_DIFFERENT,

    /**
     * Transaction encountered an error and was not retried
     */
    ERROR
}
