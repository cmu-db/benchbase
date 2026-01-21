/*
 * Copyright 2024 by BenchBase Project
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
 */

package com.oltpbenchmark.benchmarks.manav;

public abstract class ManavConstants {

  // ----------------------------------------------------------------
  // TABLE NAMES
  // ----------------------------------------------------------------
  public static final String TABLENAME_LOGS = "logs";

  // ----------------------------------------------------------------
  // BENCHMARK CONFIGURATION
  // ----------------------------------------------------------------

  // Default number of initial log entries to load
  public static final int NUM_INITIAL_LOGS = 10000;

  // ----------------------------------------------------------------
  // LOG MESSAGE CONFIGURATION
  // ----------------------------------------------------------------

  // Sample log messages for variety
  public static final String[] SAMPLE_MESSAGES = {
    "User login successful",
    "Database connection established",
    "Transaction completed successfully",
    "Cache miss occurred",
    "API request processed",
    "File upload completed",
    "Session timeout occurred",
    "Data validation passed",
    "Background job started",
    "System health check passed"
  };

  // Message prefix for generated logs
  public static final String MESSAGE_PREFIX = "Generated log entry: ";

  // Maximum length for log messages
  public static final int MAX_MESSAGE_LENGTH = 255;
}
