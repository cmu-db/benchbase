/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

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
