package com.oltpbenchmark;

public class TransactionType {

	String name;
	private int id;
	
	public TransactionType(String name, int id){
		
		this.name=name;
		this.setId(id);
		
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean isType(String string) {
		return name.equals(string);
	}
}
