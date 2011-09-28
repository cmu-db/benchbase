package com.oltpbenchmark.catalog;

import java.io.Serializable;

import org.apache.commons.lang.NotImplementedException;

/**
 * 
 * @author Carlo A. Curino (carlo@curino.us)
 */
public abstract class IntegrityConstraint implements Serializable{
    private static final long serialVersionUID = 1L;
	
	private String id;

	public IntegrityConstraint(){
	    // ????
	}
	
	@Override
	public IntegrityConstraint clone(){
		
		try {
			throw new NotImplementedException("The clone method should be implemented in the subtypes!");
		} catch (NotImplementedException e) {
			e.printStackTrace();
		}
		return null;	
	}

	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	
	public abstract boolean equals(IntegrityConstraint ic);
	

}
