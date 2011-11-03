package com.oltpbenchmark;

import java.util.List;
import java.util.Random;

import com.oltpbenchmark.tpcc.jTPCCConfig;
import com.oltpbenchmark.tpcc.jTPCCConfig.TransactionType;

public class Phase {
	
	private final Random gen = new Random();
	public int time;
	public int rate;
	public List<Double> weights;

	Phase(int t, int r, List<Double> o) {
		time = t;
		rate = r;
		weights = o;
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
		double weight = 0;
		
		for(int i=0; i<weights.size();i++){
			weight += weights.get(i);
			if(randomPercentage <= weight)
				return i;
		}
		
		return -1;
	}
	
}