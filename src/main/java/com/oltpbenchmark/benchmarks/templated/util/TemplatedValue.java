package com.oltpbenchmark.benchmarks.templated.util;

import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.JDBCSupportedType;
import com.oltpbenchmark.util.TextGenerator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random;

/**
 * This class is used to store information about the values used in templated benchmarks. It can
 * hold static values but also generators for value distributions
 */
public class TemplatedValue {
  static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  ValueGenerator distribution;
  Long min;
  Long max;
  Long seed;
  String value;
  JDBCSupportedType valueType;
  Float minF;
  Float maxF;
  Object generatorObject;

  /**
   * @param value Value that is used if no distribution is given
   */
  public TemplatedValue(String value) {
    if (value.equals("null") || value.length() < 1) {
      this.value = null;
    } else {
      this.value = value;
    }
  }

  /**
   * @param distribution The desired value distribution
   * @param min Minimum value
   * @param max Maximum value
   * @param seed The seed for the random generator. Default is 0
   * @param value Value that is used if no distribution is given
   */
  public TemplatedValue(
      String distribution, String min, String max, String seed, String valueType) {
    this.valueType = JDBCSupportedType.valueOf(valueType.toUpperCase());

    switch (this.valueType) {
      case DATE:
      case TIME:
      case TIMESTAMP:
        this.min = parseDateTime(min);
        this.max = parseDateTime(max);
        assert this.max > this.min;
        break;
      case FLOAT:
      case REAL:
        this.minF = parseBoundaryFloat(min);
        this.maxF = parseBoundaryFloat(max);
        assert this.maxF > this.minF;
        break;
      default:
        this.min = parseBoundary(min);
        this.max = parseBoundary(max);
        assert this.max > this.min;
        break;
    }

    // Parse seed with fallback of 0L
    try {
      this.seed = Long.parseLong(seed);
    } catch (NumberFormatException e) {
      this.seed = 0L;
    }

    assert this.seed >= 0L;

    this.generatorObject = createGenerator(distribution, this.min, this.max, this.seed);

    this.value = null;
  }

  /**
   * @param boundary A string signifying a numerical bondary
   * @return The numerical value parsed to a Long
   */
  private Long parseBoundary(String boundary) {
    Long lBound;
    try {
      lBound = Long.parseLong(boundary);
    } catch (NumberFormatException e) {
      throw new RuntimeException(
          String.format("Error occurred while trying to parse %s to a Long", boundary));
    }
    return lBound;
  }

  /**
   * @param boundary A string signifying a numerical bondary
   * @return The numerical value parsed to a float
   */
  private Float parseBoundaryFloat(String boundary) {
    Float fBound;
    try {
      fBound = Float.parseFloat(boundary);
    } catch (NumberFormatException e) {
      throw new RuntimeException(
          String.format("Error occurred while trying to parse %s to a Float", boundary));
    }
    return fBound;
  }

  /**
   * @param timeSource A String in a specific time format (yyyy-MM-dd HH:mm:ss) or UNIX timestamp
   * @return The UNIX timestamp of the value
   */
  private Long parseDateTime(String timeSource) {
    long timestamp;
    try {
      timestamp = Long.parseLong(timeSource);
    } catch (NumberFormatException ex) {
      try {
        timestamp = dateFormat.parse(timeSource).getTime();
      } catch (ParseException e) {
        throw new RuntimeException(
            String.format("Error occurred while trying to parse date: %s", timeSource));
      }
    }
    return timestamp;
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

    this.distribution = ValueGenerator.valueOf(distribution.toUpperCase());

    switch (this.distribution) {
      case UNIFORM:
        this.distribution = ValueGenerator.UNIFORM;
        return new Random(seed);
      case NORMAL:
        this.distribution = ValueGenerator.NORMAL;
        return new Random(seed);
      case ZIPFIAN:
        this.distribution = ValueGenerator.ZIPFIAN;
        return new ZipfianGenerator(new Random(seed), min, max);
      case SCRAMBLED:
        this.distribution = ValueGenerator.SCRAMBLED;
        return new ScrambledZipfianGenerator(min, max);
      default:
        throw new RuntimeException(
            "The distribution: '"
                + distribution
                + "' is not supported. Currently supported are 'uniform' | 'normal' | 'zipfian' | 'scrambled'");
    }
  }

  public ValueGenerator getDistribution() {
    return this.distribution;
  }

  public String getMin() {
    return (this.min != null) ? this.min.toString() : this.minF.toString();
  }

  public String getMax() {
    return (this.max != null) ? this.max.toString() : this.maxF.toString();
  }

  public String getSeed() {
    return this.seed.toString();
  }

  public String getValue() {
    return this.value;
  }

  public String getValueType() {
    return this.valueType.toString();
  }

  public Object getGenerator() {
    return this.generatorObject;
  }

  public Long getNextLongBinomial() {
    assert this.valueType.equals(JDBCSupportedType.INTEGER)
        || this.valueType.equals(JDBCSupportedType.BIGINT)
        || this.valueType.equals(JDBCSupportedType.DATE)
        || this.valueType.equals(JDBCSupportedType.TIME)
        || this.valueType.equals(JDBCSupportedType.TIMESTAMP);
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
    assert this.valueType.equals(JDBCSupportedType.INTEGER)
        || this.valueType.equals(JDBCSupportedType.BIGINT)
        || this.valueType.equals(JDBCSupportedType.DATE)
        || this.valueType.equals(JDBCSupportedType.TIME)
        || this.valueType.equals(JDBCSupportedType.TIMESTAMP);
    Random uniformGenerator = (Random) this.generatorObject;
    return uniformGenerator.nextLong(this.min, this.max);
  }

  public Long getNextLongZipf() {
    assert this.valueType.equals(JDBCSupportedType.INTEGER)
        || this.valueType.equals(JDBCSupportedType.BIGINT)
        || this.valueType.equals(JDBCSupportedType.DATE)
        || this.valueType.equals(JDBCSupportedType.TIME)
        || this.valueType.equals(JDBCSupportedType.TIMESTAMP);
    ZipfianGenerator zipfianGenerator = (ZipfianGenerator) this.generatorObject;
    return zipfianGenerator.nextLong();
  }

  public Long getNextLongScrambled() {
    assert this.valueType.equals(JDBCSupportedType.INTEGER)
        || this.valueType.equals(JDBCSupportedType.BIGINT)
        || this.valueType.equals(JDBCSupportedType.DATE)
        || this.valueType.equals(JDBCSupportedType.TIME)
        || this.valueType.equals(JDBCSupportedType.TIMESTAMP);
    ScrambledZipfianGenerator scrambledGenerator = (ScrambledZipfianGenerator) this.generatorObject;
    return scrambledGenerator.nextLong();
  }

  public String getNextString() {
    assert this.valueType.equals(JDBCSupportedType.VARCHAR)
        || this.valueType.equals(JDBCSupportedType.CHAR);
    Random stringGenerator = (Random) this.generatorObject;
    return TextGenerator.randomStr(stringGenerator, this.max.intValue());
  }

  public Float getNextFloatUniform() {
    assert this.valueType.equals(JDBCSupportedType.REAL)
        || this.valueType.equals(JDBCSupportedType.FLOAT);
    Random floatGenerator = (Random) this.generatorObject;
    return floatGenerator.nextFloat(this.minF, this.maxF);
  }

  public Float getNextFloatBinomial() {
    assert this.valueType.equals(JDBCSupportedType.REAL)
        || this.valueType.equals(JDBCSupportedType.FLOAT);
    Random floatGenerator = (Random) this.generatorObject;
    Float generatedFloat;
    do {
      generatedFloat = (float) (this.minF + Math.abs(floatGenerator.nextGaussian()) * this.maxF);
    } while (generatedFloat > this.maxF || generatedFloat < this.minF);

    return generatedFloat;
  }

  public RuntimeException createRuntimeException() {
    return new RuntimeException(
        "Distribution: " + this.distribution + " not supported for type: " + getValueType());
  }
}
