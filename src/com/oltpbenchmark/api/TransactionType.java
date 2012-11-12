package com.oltpbenchmark.api;


public class TransactionType implements Comparable<TransactionType> {

    public static class Invalid extends Procedure { }
    public static final int INVALID_ID = 0;
    public static final TransactionType INVALID = new TransactionType(Invalid.class, INVALID_ID);
    
	private final Class<? extends Procedure> procClass;
	private final int id;
	private final boolean supplemental;

	protected TransactionType(Class<? extends Procedure> procClass, int id, boolean supplemental) {
	    this.procClass = procClass;
        this.id = id;
        this.supplemental = supplemental;
    }
	
	protected TransactionType(Class<? extends Procedure> procClass, int id) {
	    this(procClass, id, false);
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
	public boolean isSupplemental() {
        return this.supplemental;
    }

	@Override
	public boolean equals(Object obj) {
	    if (this == obj)
	        return true;
	    
	    if (!(obj instanceof TransactionType) || obj == null)
	        return false;
	    
		TransactionType other = (TransactionType)obj;
		return (this.id == other.id && this.procClass.equals(other.procClass));
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
