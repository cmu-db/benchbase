/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.util;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public class MeasureTargetSystem implements Runnable {

	BufferedWriter out, out2 = null;
	StatisticsCollector sc = null;
	int intermediateWarmupTime, measuringTime, speed;
	long lastPhaseChange;
	StatsHolder s;

	public MeasureTargetSystem(BufferedWriter out, BufferedWriter out2,
			StatisticsCollector sc, int intermediateWarmupTime,
			int measuringTime) {
		this.out = out;
		this.out2 = out2;
		this.sc = sc;
		this.intermediateWarmupTime = intermediateWarmupTime;
		this.measuringTime = measuringTime;
		this.lastPhaseChange = System.currentTimeMillis();

		s = new StatsHolder(13);
	}

	public void setSpeed(int speed) throws IOException {
		this.speed = speed;
		this.lastPhaseChange = System.currentTimeMillis();

		double[] t2 = s.getAverage();
		out2.write(buildoutString("measure", 99999999, t2));
		out2.flush();
		s.reset();
	}

	@Override
	public void run() {

		try {

			out.write("phase_of_test\t requested_speed(tps)\t measured_speed(rows/sec) \t cpu \t read_per_sec \t read_merged_per_sec \t sector_reads_per_sec \t %_disk_read \t write_per_sec \t write_merged_per_sec \t sector_written_per_sec \t %_disk_write \t io_currently_in_progress \t %_time_disk_util \t %_weighted_time_disk_util \n");

			// initial warmup phase
			while (true) {
				double[] t = sc.getStats();
				long curr = System.currentTimeMillis();
				if (curr - lastPhaseChange < intermediateWarmupTime * 1000)
					out.write(buildoutString("warmup", speed, t));
				else {
					out.write(buildoutString("measure", speed, t));
					s.add(t);
				}
				out.flush();
				Thread.sleep(5000);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static String buildoutString(String phase, double nextTargetTPS,
			double[] t) {

	    StringBuilder outt = new StringBuilder();
		outt.append(phase + "\t " + nextTargetTPS);

		for (int i = 0; i < t.length; i++)
			outt.append("\t" + t[i]);

		outt.append(outt + "\n");
		return outt.toString();

	}

}
