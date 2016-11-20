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
