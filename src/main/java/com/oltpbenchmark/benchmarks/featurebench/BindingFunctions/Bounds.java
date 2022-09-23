package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

/**
 * Easily step from one value to the next according to a modified
 * logarithmic sequence that makes it easy to pick useful testing
 * boundaries.
 * <p>
 * With levels per magnitude at 1, the progression goes in powers
 * of 10. With any higher value than 1, each magnitude is divided
 * into equal parts. For example, starting at 10 with 2 levels per magnitude,
 * you get 50, 100, 500, 1000, 5000, and so on when you ask for
 * the next higher bound.
 */
public class Bounds {

    private final int levelsPerMagnitude;
    private long currentValue;

    public Bounds(long startingValue, int levelsPerMagnitude) {
        this.currentValue = startingValue;
        this.levelsPerMagnitude = levelsPerMagnitude;
    }

    public Bounds setValue(long value) {
        this.currentValue = value;
        return this;
    }

    public long getValue() {
        return currentValue;
    }

    public long getNextValue() {
        long nextValue = findNextHigherValue();
        currentValue = nextValue;
        return currentValue;
    }

    private long findNextHigherValue() {
        int pow10 = (int) Math.log10(currentValue);
        if (levelsPerMagnitude == 1) {
            return (long) Math.pow(10, pow10 + 1);
        }
        double baseMagnitude = Math.pow(10, pow10);
        double increment = baseMagnitude / levelsPerMagnitude;

        long newValue = (long) (currentValue + increment);
        return newValue;
    }

    @Override
    public String toString() {
        return this.currentValue + "(incr by 1/" + this.levelsPerMagnitude + ")";
    }
}
