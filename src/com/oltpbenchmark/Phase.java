package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.TransactionType;

public class Phase {
	
	private final Random gen = new Random();
	public int time;
	public int rate;
	public List<Double> weights;

	Phase(int t, int r, List<String> o) {
		
		ArrayList<Double> w= new ArrayList<Double>();
		
		for(String s:o)
			w.add(Double.parseDouble(s));
		
		time = t;
		rate = r;
		weights = w;
	}

	/**
	 * Computes the sum of weights.
	 * Usually needs to add up to 100%
	 * @return The total weight
	 */
	public double totalWeight() {
		double total=0.0;
		for(Double d:weights)
			total+=d;
		return total;
	}
	
	/**
	 * This simply computes the next transaction by randomly selecting one based on the weights of this phase.
	 * @return
	 */
	public int chooseTransaction() {
		
		int randomPercentage = gen.nextInt(100) + 1;
		Double weight = 0.0;
		
		for(int i=0; i<weights.size();i++){
			weight += weights.get(i);
			if(randomPercentage <= weight)
				return i;
		}
		
		return -1;
	}
	
}