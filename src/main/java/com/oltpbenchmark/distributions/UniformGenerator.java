package com.oltpbenchmark.distributions;

import java.util.concurrent.ThreadLocalRandom;

public class UniformGenerator extends IntegerGenerator {
  int min;
  int max;

  /**
   * Create a uniformly distributed random number generator for items.
   *
   * @param items Number of items.
   */
  public UniformGenerator(int items) {
    this(0, items - 1);
  }

  /**
   * Create a uniformly distributed random number generator for items between min and max
   * (inclusive).
   *
   * @param min Smallest integer to generate in the sequence.
   * @param max Largest integer to generate in the sequence.
   */
  public UniformGenerator(int min, int max) {
    this.min = min;
    this.max = max;
  }

  @Override
  public int nextInt() {
    return ThreadLocalRandom.current().nextInt(this.min, this.max + 1);
  }

  @Override
  public double mean() {
    return (this.max + this.min) / 2.0;
  }
}
