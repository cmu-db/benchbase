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

import java.util.*;

import javax.persistence.*;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.jpab.tests.Test;

/**
 * A simple sub entity class (two inheritance levels).
 */
@Entity
public class Person2d extends Person1st implements TestEntity {
	
	// Fields:

	@Temporal(TemporalType.DATE)
	private Date birthDate;
	@Temporal(TemporalType.DATE)
	private Date joinDate;

	@Temporal(TemporalType.DATE)
	private Date lastLoginDate;

	@Basic private int loginCount;

	// Constructors:

    public Person2d() {
    	// used by JPA to load an entity object from the database
    }

    public Person2d(Test test) {
    	super(test);
    	Date[] dates = null;
    	birthDate = null;//dates[0];
    	joinDate =  null;//dates[1];
    	lastLoginDate = null;//dates[2]; 
    	loginCount = LoaderUtil.randomNumber(1, 100, new Random());
    }

	// Methods:

    @Override
	public void load() {
    	super.load();
		assert birthDate != null && joinDate != null &&
			lastLoginDate != null && loginCount > 0;
    }

    public void update() {
    	lastLoginDate = new Date();
    	loginCount++;
    }
}
