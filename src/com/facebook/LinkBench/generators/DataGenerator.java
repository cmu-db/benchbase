/*
 * Copyright 2012, Facebook, Inc.
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
package com.facebook.LinkBench.generators;

import java.util.Properties;
import java.util.Random;

public interface DataGenerator {

  public void init(Properties props, String keyPrefix);

  /**
   * Fill the provided array with randomly generated data
   * @param data
   * @return the argument, as a convenience so that an array can be
   *    constructed and filled in a single statement
   */
  public byte[] fill(Random rng, byte data[]);
}
