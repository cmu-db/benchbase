package com.oltpbenchmark;

import java.util.ArrayList;

public class TransactionTypes {
	
	ArrayList<TransactionType> transactiontypes;
	public TransactionTypes(ArrayList<TransactionType> transactiontypes){
		this.transactiontypes = transactiontypes;
		
	}
	
	public TransactionType getType(String name){
		
		for(TransactionType t:transactiontypes)
			if(t.name.equals(name))
				return t;
		return null;
		
	}

	public TransactionType getType(int id) {
		for(TransactionType t:transactiontypes)
			if(t.getId() == id)
				return t;
		return null;
	}
	
	
}
