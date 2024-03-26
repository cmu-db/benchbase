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
package com.oltpbenchmark.benchmarks.templated.util;

import com.oltpbenchmark.api.Operation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Immutable class containing information about transactions. */
public class GenericQueryOperation extends Operation {

  public final List<TemplatedValue> params;

  public GenericQueryOperation(TemplatedValue[] params) {
    super();
    this.params = Collections.unmodifiableList(Arrays.asList(params));
  }

  public List<TemplatedValue> getParams() {
    return params;
  }
}
