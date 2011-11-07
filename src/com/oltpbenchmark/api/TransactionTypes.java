package com.oltpbenchmark.api;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections15.map.ListOrderedMap;


public class TransactionTypes {
	
	private final ListOrderedMap<String, TransactionType> types = new ListOrderedMap<String, TransactionType>();
	
	public TransactionTypes(List<TransactionType> transactiontypes) {
		Collections.sort(transactiontypes, new Comparator<TransactionType>() {
			@Override
			public int compare(TransactionType o1, TransactionType o2) {
				return o1.compareTo(o2);
			}
		});
		for (TransactionType tt : transactiontypes) {
			this.types.put(tt.getName().toUpperCase(), tt);
		} // FOR
	}
	
	public TransactionType getType(String name) {
		return (this.types.get(name.toUpperCase()));
	}

	public TransactionType getType(int id) {
		return (this.types.getValue(id));
	}
	
	@Override
	public String toString() {
		return this.types.values().toString();
	}
	
}
