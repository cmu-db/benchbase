package com.oltpbenchmark.api;


public class TransactionType implements Comparable<TransactionType> {

    public static final int INVALID_ID = 0;
    public static final String INVALID_NAME = "INVALID";
    public static final TransactionType INVALID = new TransactionType(INVALID_NAME, INVALID_ID);
    
	private final String name;
	private final int id;

	public TransactionType(String name, int id) {
		this.name = name;
		this.id = id;
	}

	public String getName() {
		return this.name;
	}
	public int getId() {
		return this.id;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TransactionType) {
			TransactionType other = (TransactionType)obj;
			return (this.id == other.id && this.name.equals(other.name));
		}
		return (false);
	}
	
	public boolean equals(Class<? extends Procedure> clazz) {
		return clazz.getSimpleName().equalsIgnoreCase(this.name);
	}
	
	public boolean equals(String string) {
		return this.name.equalsIgnoreCase(string);
	}

	@Override
	public int hashCode() {
        return (this.id * 31) + this.name.hashCode();
	}
	
	@Override
	public int compareTo(TransactionType o) {
		return (this.id - o.id);
	}
	
	@Override
	public String toString() {
		return String.format("%s/%02d", this.name, this.id);
	}
	
}
