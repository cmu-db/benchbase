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

/**
 * Base Catalog Object Class
 * @author pavlo
 */
public abstract class AbstractCatalogObject implements Serializable {
	static final long serialVersionUID = 0;
	
	protected final String name;
	
	public AbstractCatalogObject(String name) {
		this.name = name;
	}
	
	/**
	 * Return the name of this catalog object in the database
	 * @return
	 */
	public final String getName() {
		return (this.name);
	}
	
	/**
     * Return the name of this catalog object escaped with the 
     * by the CatalogUtil.separator
     * @return
     */
    public final String getEscapedName() {
    	String s = Catalog.getSeparator();
    	return s + this.name + s;
    }
}
