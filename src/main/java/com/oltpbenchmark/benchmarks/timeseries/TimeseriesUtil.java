package com.oltpbenchmark.benchmarks.timeseries;

import java.time.LocalDateTime;

public class TimeseriesUtil {

    /**
     * For a given source_id, return the starting timestamp for any
     * session/observation in the database.
     * @param source_id
     * @return
     */
    public static LocalDateTime getCreateDateTime(int source_id) {
        return TimeseriesConstants.START_DATE.plusHours(source_id);
    }

    /**
     * For a given source_id and timetick within the session, return the timestamp
     * for the observations
     * @param source_id
     * @param timetick
     * @return
     */
    public static LocalDateTime getObservationDateTime(int source_id, int timetick) {
        LocalDateTime base = getCreateDateTime(source_id);
        return base.plusMinutes(timetick * 20);
    }



}
