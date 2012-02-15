/*
 * JPA Performance Benchmark - http://www.jpab.org
 * Copyright ObjectDB Software Ltd. All Rights Reserved. 
 *
 * This is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation; either version 2 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA.
 */

package com.oltpbenchmark.benchmarks.jpab.objects;

import javax.persistence.*;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

/**
 * A simple sub entity class (one inheritance level).
 */
@Entity
public class Person1st extends PersonBase {
	
	// Fields:

	private String street;
	private String city;
	private String state;
	private String zip;
	private String country;
	private String phone;
	private String email;

	// Constructors:

    public Person1st() {
    	// used by JPA to load an entity object from the database
    }

	// Methods:

    public Person1st(Test test) {
    	super(test);
    	street = LoaderUtil.randomStr(10);
    	city = LoaderUtil.randomStr(10);
    	state = LoaderUtil.randomStr(10);
    	zip = LoaderUtil.randomStr(10);
    	country = LoaderUtil.randomStr(10);
    	phone = LoaderUtil.randomStr(10);
    	email = LoaderUtil.randomStr(10);
    }
    
    @Override
	public void load() {
    	super.load();
		assert street != null && city != null && state != null &&
			zip != null && country != null && phone != null && email != null;
    }
}
