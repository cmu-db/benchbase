/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

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
package com.oltpbenchmark.benchmarks.linkbench.utils;

import java.util.Random;

/**
 * Shuffler designed to make computing permutation and inverse easy
 */
public class InvertibleShuffler {
  private final long[] params;
  private final int shuffleGroups;
  long n;
  long nRoundedUp; // n rounded up to next multiple of shuffleGroups
  long nRoundedDown; // n rounded down to next multiple of shuffleGroups
  int minGroupSize;

  public InvertibleShuffler(long seed, int shuffleGroups, long n) {
    this(new Random(seed), shuffleGroups, n);
  }
  public InvertibleShuffler(Random rng, int shuffleGroups, long n) {
    if (shuffleGroups > n) {
      // Can't have more shuffle groups than items
      shuffleGroups = (int)n;
    }
    this.shuffleGroups = shuffleGroups;
    this.n = n;
    this.params = new long[shuffleGroups];
    this.minGroupSize = (int)n / shuffleGroups;

    for (int i = 0; i < shuffleGroups; i++) {
      // Positive long
      params[i] = Math.abs(rng.nextInt(minGroupSize));
    }
    this.nRoundedDown = (n / shuffleGroups) * shuffleGroups;
    this.nRoundedUp = n == nRoundedDown ? n : nRoundedDown + shuffleGroups;
  }

  public long permute(long i) {
    return permute(i, false);
  }

  public long invertPermute(long i) {
    return permute(i, true);
  }

  public long permute(long i, boolean inverse) {
    if (i < 0 || i >= n) {
      throw new IllegalArgumentException("Bad index to permute: " + i
          + ": out of range [0:" + (n - 1) + "]");
    }
    // Number of the group
    int group = (int) (i % shuffleGroups);

    // Whether this is a big or small group
    boolean bigGroup = group < n % shuffleGroups;

    // Calculate the (positive) rotation
    long rotate = params[group];
    if (inverse) {
      // Reverse the rotation
      if (bigGroup) {
        rotate = minGroupSize + 1 - rotate;
      } else {
        rotate = minGroupSize - rotate;
      }
      assert(rotate >= 0);
    }

    long j = (i + shuffleGroups * rotate);
    long result;
    if (j < n) {
      result = j;
    } else {
      // Depending on the group there might be different numbers of
      // ids in the ring
      if (bigGroup) {
        result = j % nRoundedUp;
      } else {
        result = j % nRoundedDown;
      }
      if (result >= n) {
        result = group;
      }
    }
    assert(result % shuffleGroups == group);
    return result;
  }

}
