package com.oltpbenchmark;

import java.util.ArrayList;

public class ReplayPhase extends Phase {
    public ReplayPhase() {
        // it's important to set rateLimited to true, disabled to false, and serial to false so that we do use the workQueue
        // this is not just a "hack" because a replay phase is conceptually rate limited, not disabled, and not serial
        super("", 0, 0, 0, 0, new ArrayList<Double>(), true, false, false, false, 0, Arrival.REGULAR);
    }

    @Override
    public int chooseTransaction() {
        return 5;
    }
}
