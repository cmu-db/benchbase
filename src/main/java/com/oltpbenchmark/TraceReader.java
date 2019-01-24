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

package com.oltpbenchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.List;
import java.util.LinkedList;

import org.apache.log4j.Logger;

/**
 * This class reads in one of the tracefiles (.raw output from a previous
 * benchmark run) and supplies a list of transactions and start times to
 * WorkloadState, in order to repeat a given execution of a benchmark.
 *
 * @author breilly
 */
public class TraceReader {
    private static final Logger LOG = Logger.getLogger(TraceReader.class);

    // POD for tracking submitted/read procedures.
    private class TraceElement {
        int txnId;
        int phaseId;
        long startTimeNs;

        public TraceElement(int txnId, int phaseId, long startTimeNs) {
            this.txnId = txnId;
            this.phaseId = phaseId;
            this.startTimeNs = startTimeNs;
        }
    }

    private LinkedList<TraceElement> tracedProcedures = new LinkedList<TraceElement>();
    private String tracefileName;
    private int currentPhaseId;
    private long phaseStartTime;
    private boolean phaseComplete;

    public boolean getPhaseComplete() {
        return phaseComplete;
    }

    /**
     * Takes as input a trace file (.raw file from a previous run), and stores
     * the transaction id, phase id, and transaction start time for each one.
     */
    public TraceReader(String filename) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line = br.readLine();

            if (line == null) {
                LOG.error("Trace file " + filename + "is empty.");
                System.exit(1);
            }

            // First line indicates the column titles. Using these titles to
            // determine which columns to look at gives us flexibility in case
            // the ordering changes or extra columns are added.
            String[] splitHeader = line.split(",");
            int txnIdCol = -1, phaseIdCol = -1, startTimeCol = -1;
            int index = 0;
            for (String field : splitHeader) {
                if(field.matches(".*transaction.*"))
                    txnIdCol = index;
                else if(field.matches(".*phase.*"))
                    phaseIdCol = index;
                else if(field.matches(".*start time.*"))
                    startTimeCol = index;
                ++index;
            }

            // If any of the columns were not found, then die.
            LOG.info("Parsing trace file using indexes: "
                     + txnIdCol + "," + phaseIdCol + "," + startTimeCol);
            if (txnIdCol < 0 || phaseIdCol < 0 || startTimeCol < 0) {
                LOG.error("Could not understand column headers in trace file.");
                System.exit(1);
            }


            // Now iterate through the whole file, parsing transaction info
            // line-by-line to create a list of procedures to run.
            try {
                int currPhaseId = -1;
                long phaseBaseTime = 0;
                while ((line = br.readLine()) != null) {
                    String[] splitLine = line.split(",");
                    int phaseId = Integer.parseInt(splitLine[phaseIdCol]);
                    long startTimeNs = (long)(1000*1000*1000
                                       * Double.parseDouble(splitLine[startTimeCol]));

                    // We base transaction start times on the start of a phase
                    if (phaseId != currPhaseId) {
                        assert phaseId > currPhaseId;
                        currPhaseId = phaseId;
                        phaseBaseTime = startTimeNs;
                    }

                    // Create the new procedure according to the entry in the
                    // trace file.
                    assert phaseBaseTime <= startTimeNs;
                    tracedProcedures.add(new TraceElement(
                                             Integer.parseInt(splitLine[txnIdCol])
                                             , phaseId
                                             , startTimeNs - phaseBaseTime));
                }
            }
            catch (Exception e) {
                LOG.error("Encountered a bad line in the trace file: " + line);
                LOG.error(e.getMessage());
            }
        }
        catch(Exception e) {
            LOG.error(e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Returns a list of procedures that should be submitted to the work queue.
     *
     * @throws IncompletePhaseException
     */
    public LinkedList<SubmittedProcedure> getProcedures(long nowNs)
    {
        long timeSincePhaseStart = nowNs - phaseStartTime;
        // Nothing to do if the list is empty.
        LinkedList<SubmittedProcedure> readyProcedures = new LinkedList<SubmittedProcedure>();
        if (tracedProcedures.isEmpty()) {
            phaseComplete = true;
            return readyProcedures;
        }

        ListIterator<TraceElement> iter = tracedProcedures.listIterator();
        TraceElement curr = tracedProcedures.peek();

        // Shouldn't have a procedure from a previous phase, or else we
        // wouldn't have switched phases successfully.
        assert curr.phaseId >= currentPhaseId;

        // Loop through the procedures until we find one that's out of the
        // phase or within the current phase but beyond the time marker.
        while (iter.hasNext()) {
            curr = iter.next();
            if (curr.phaseId != currentPhaseId
                || curr.startTimeNs > timeSincePhaseStart)
            {
                break;
            }
            readyProcedures.add(new SubmittedProcedure(curr.txnId, nowNs));
            iter.remove();
        }

        // If the list is now empty or the next procedure isn't from this
        // phase, then we're out of procedures for the current phase.
        if (tracedProcedures.size() == 0
            || tracedProcedures.peek().phaseId != currentPhaseId)
        {
            phaseComplete = true;
        }

        return readyProcedures;
    }

    /**
     * Lets the TraceReader know that a new phase has begun.
     */
    public void changePhase(int newPhaseId, long phaseStartTime)
    {
        // Should only change the phase if our list indicates that there are no
        // remaining procedures from earlier phases.
        TraceElement head = tracedProcedures.peek();
        if (head.phaseId < newPhaseId) {
            LOG.error("Changing to phase " + newPhaseId
                      + " but head procedure is from"
                      + " phase" + head.phaseId + ".");
            System.exit(1);
        }

        // Everything looks okay, so let's acknowledge the change.
        currentPhaseId = newPhaseId;
        phaseComplete = false;
        this.phaseStartTime = phaseStartTime;
    }

    /**
     * Converts the list of procedures to a CSV string for easy validation.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder(10*tracedProcedures.size());
        sb.append("TraceReader");
        for(TraceElement t : tracedProcedures) {
            sb.append("\n");
            sb.append(t.txnId);
            sb.append(",");
            sb.append(t.phaseId);
            sb.append(",");
            sb.append(t.startTimeNs);
        }

        return sb.toString();
    }
}
