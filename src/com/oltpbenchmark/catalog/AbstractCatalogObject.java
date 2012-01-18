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
