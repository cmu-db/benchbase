package com.oltpbenchmark.benchmarks.templated.util;

import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.TextGenerator;
import java.util.Random;

/**
 * This class is used to store information about the values used in templated benchmarks. It can
 * hold static values but also generators for value distributions
 */
public class TemplatedValue {
  ValueGenerator distribution;
  Long min;
  Long max;
  Long seed;
  String value;
  String minS;
  String maxS;
  Object generatorObject;

  /**
   * @param value Value that is used if no distribution is given
   */
  public TemplatedValue(String value) {
    this.value = value;
  }

  /**
   * @param distribution The desired value distribution
   * @param min Minimum value. Default is 0
   * @param max Maximum value. Default is min + 1
   * @param seed The seed for the random generator. Default is 0
   * @param value Value that is used if no distribution is given
   */
  public TemplatedValue(String distribution, String min, String max, String seed) {
    try {
      this.min = Long.parseLong(min);
    } catch (Exception e) {
      this.min = 0L;
    }

    try {
      this.max = Long.parseLong(max);
    } catch (Exception e) {
      this.max = this.min + 1L;
    }

    assert this.max > this.min;

    try {
      this.seed = Long.parseLong(seed);
    } catch (Exception e) {
      this.seed = 0L;
    }

    this.generatorObject = createGenerator(distribution, this.min, this.max, this.seed);

    // String values are kept for the construction of Floating Point bounds
    this.minS = min;
    this.maxS = max;
  }

  /**
   * @param distribution The distribution that drives the generator
   * @param min Minimum value the generator will produce
   * @param max Maximum value the generator will produce
   * @param seed A seed for the generator
   * @return
   */
  private Object createGenerator(String distribution, Long min, Long max, Long seed) {

    if (distribution == null || distribution.equals("null") || distribution.length() < 1) {
      return null;
    }

    switch (distribution.toLowerCase()) {
      case "uniform":
        this.distribution = ValueGenerator.UNIFORM;
        return new Random(seed);
      case "binomial":
      case "normal":
        this.distribution = ValueGenerator.BINOMIAL;
        return new Random(seed);
      case "zipf":
      case "zipfian":
        this.distribution = ValueGenerator.ZIPFIAN;
        return new ZipfianGenerator(new Random(seed), min, max);
      case "scrambled":
      case "scramzipf":
        this.distribution = ValueGenerator.SCRAMBLED;
        return new ScrambledZipfianGenerator(min, max);
      default:
        throw new RuntimeException(
            "The distribution: '"
                + distribution
                + "' is not supported. Currently supported are 'uniform' | 'binomial' | 'zipfian' | 'scrambled'");
    }
  }

  public ValueGenerator getDistribution() {
    return this.distribution;
  }

  public Long getMin() {
    return this.min;
  }

  public Long getMax() {
    return this.max;
  }

  public Long getSeed() {
    return this.seed;
  }

  public String getValue() {
    return this.value;
  }

  public Object getGenerator() {
    return this.generatorObject;
  }

  public Long getNextLongBinomial() {
    Random binomialGenerator = (Random) this.generatorObject;
    Long generatedValue;
    do {
      generatedValue =
          Double.valueOf(this.min + Math.abs(binomialGenerator.nextGaussian()) * this.max)
              .longValue();
    } while (generatedValue > max || generatedValue < min);

    return generatedValue;
  }

  public Long getNextLongUniform() {
    Random uniformGenerator = (Random) this.generatorObject;
    return uniformGenerator.nextLong(this.min, this.max);
  }

  public Long getNextLongZipf() {
    ZipfianGenerator zipfianGenerator = (ZipfianGenerator) this.generatorObject;
    return zipfianGenerator.nextLong();
  }

  public Long getNextLongScrambled() {
    ScrambledZipfianGenerator scrambledGenerator = (ScrambledZipfianGenerator) this.generatorObject;
    return scrambledGenerator.nextLong();
  }

  public String getNextString() {
    Random stringGenerator = (Random) this.generatorObject;
    return TextGenerator.randomStr(stringGenerator, this.max.intValue());
  }

  public Float getNextFloatUniform() {
    Random floatGenerator = (Random) this.generatorObject;
    return floatGenerator.nextFloat(Float.parseFloat(this.minS), Float.parseFloat(this.maxS));
  }

  public Float getNextFloatBinomial() {
    Float minF = Float.parseFloat(this.minS);
    Float maxF = Float.parseFloat(this.maxS);
    Random floatGenerator = (Random) this.generatorObject;
    Float generatedFloat;
    do {
      generatedFloat = (float) (minF + Math.abs(floatGenerator.nextGaussian()) * maxF);
    } while (generatedFloat > maxF || generatedFloat < minF);

    return generatedFloat;
  }

  public RuntimeException createRuntimeException(String paramType) {
    return new RuntimeException(
        "Distribution: " + this.distribution + " not supported for type: " + paramType);
  }
}
