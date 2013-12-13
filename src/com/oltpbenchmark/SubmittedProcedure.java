package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;

import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.QueueLimitException;
import org.apache.log4j.Logger;

/**
 * This class is used for keeping track of the procedures that have been
 * submitted to the system when running a rate-limited benchmark.
 * @author breilly
 */
public class SubmittedProcedure {
    private final int type;
    private final long startTime;

    SubmittedProcedure(int type) {
        this.type = type;
        this.startTime = System.nanoTime();
    }

    SubmittedProcedure(int type, long startTime) {
        this.type = type;
        this.startTime = startTime;
    }

    public int getType() { return type; }
    public long getStartTime() { return startTime; }
}
