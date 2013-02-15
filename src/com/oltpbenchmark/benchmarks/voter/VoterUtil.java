package com.oltpbenchmark.benchmarks.voter;

public class VoterUtil {
    /**
     * Return the number of contestants to use for the given scale factor
     * @param scaleFactor
     */
    public static int getScaledNumContestants(double scaleFactor) {
        int min_contestants = 1;
        int max_contestants = VoterConstants.CONTESTANT_NAMES_CSV.split(",").length;

        int num_contestants = (int)Math.round(VoterConstants.NUM_CONTESTANTS * scaleFactor);
        if (num_contestants < min_contestants) num_contestants = min_contestants;
        if (num_contestants > max_contestants) num_contestants = max_contestants;

        return (num_contestants);
    }
}
