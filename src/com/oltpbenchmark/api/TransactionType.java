package com.oltpbenchmark.api;


public class TransactionType implements Comparable<TransactionType> {

    public static class Invalid extends Procedure { }
    public static final int INVALID_ID = 0;
    public static final TransactionType INVALID = new TransactionType(Invalid.class, INVALID_ID);
    
	private final Class<? extends Procedure> procClass;
	private final int id;

	protected TransactionType(Class<? extends Procedure> procClass, int id) {
		this.procClass = procClass;
		this.id = id;
	}

	public Class<? extends Procedure> getProcedureClass() {
	    return (this.procClass);
	}
	public String getName() {
		return this.procClass.getSimpleName();
	}
	public int getId() {
		return this.id;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TransactionType) {
			TransactionType other = (TransactionType)obj;
			return (this.id == other.id && this.procClass.equals(other.procClass));
		}
		return (false);
	}
	
	
	@Override
	public int hashCode() {
        return (this.id * 31) + this.procClass.hashCode();
	}
	
	@Override
	public int compareTo(TransactionType o) {
		return (this.id - o.id);
	}
	
	@Override
	public String toString() {
		return String.format("%s/%02d", this.procClass.getSimpleName(), this.id);
	}
	
}
