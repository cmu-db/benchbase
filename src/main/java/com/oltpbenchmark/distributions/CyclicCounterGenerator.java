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
package com.oltpbenchmark.distributions;

import java.util.concurrent.atomic.AtomicInteger;

/** Thread-safe cyclic counter generator. */
public class CyclicCounterGenerator extends IntegerGenerator {

  private final int maxVal;
  private final AtomicInteger counter;

  public CyclicCounterGenerator(int maxVal) {
    this.maxVal = maxVal;
    this.counter = new AtomicInteger(0);
  }

  protected void setLastInt(int last) {
    throw new UnsupportedOperationException("Cyclic counter cannot be set to a value");
  }

  @Override
  public int nextInt() {
    return counter.accumulateAndGet(1, (index, inc) -> (++index >= maxVal ? 0 : index));
  }

  @Override
  public String nextString() {
    return "" + nextInt();
  }

  @Override
  public String lastString() {
    return "" + lastInt();
  }

  @Override
  public int lastInt() {
    return counter.get();
  }

  @Override
  public double mean() {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
