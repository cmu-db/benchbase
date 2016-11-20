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
 * A simple entity class with one index.
 */
@Entity
@TableGenerator(name="indexSeq", allocationSize=1000)
public class IndexedPerson implements TestEntity {
	
	// Fields:

	@Id @GeneratedValue(strategy=GenerationType.TABLE, generator="indexSeq")
    private Integer id;

	private String firstName;
	private String middleName;
	
	@javax.jdo.annotations.Index
    @org.hibernate.annotations.Index(name="lastName")
    @org.apache.openjpa.persistence.jdbc.Index
	private String lastName;
	
	private String street;
	private String city;
	private String state;
	private String zip;
	private String country;
	private String phone;
	private String email;

	@Temporal(TemporalType.DATE)
	private Date birthDate;
	@Temporal(TemporalType.DATE)
	private Date joinDate;
	@Temporal(TemporalType.DATE)
	private Date lastLoginDate;

	@Basic private int loginCount;

	// Constructors:

    public IndexedPerson() {
    	// used by JPA to load an entity object from the database
    }

    public IndexedPerson(Test test) {
    	firstName = LoaderUtil.randomStr(10);
    	middleName = LoaderUtil.randomStr(10);
    	lastName = LoaderUtil.randomStr(10);
    	street = LoaderUtil.randomStr(10);
    	city =LoaderUtil.randomStr(10);
    	state = LoaderUtil.randomStr(10);
    	country = LoaderUtil.randomStr(10);
    	zip = LoaderUtil.randomStr(10);
    	phone = LoaderUtil.randomStr(10);
    	email = LoaderUtil.randomStr(10);
    	Date[] dates = null;
    	birthDate = null;//dates[0];
    	joinDate = null;// dates[1];
    	lastLoginDate = null;//dates[2]; 
    	loginCount = LoaderUtil.randomNumber(1, 100, new Random());
    }

	// Methods:

    public void load() {
		assert firstName != null && middleName != null && lastName != null &&
			street != null && city != null && state != null &&
			zip != null && country != null && phone != null && email != null &&
			birthDate != null && joinDate != null &&
			lastLoginDate != null && loginCount > 0;
    }

    public void update() {
    	lastLoginDate = new Date();
    	loginCount++;
    }

    @Override
    public String toString() {
    	StringBuilder sb = new StringBuilder(64);
    	sb.append(firstName);
    	if (middleName != null) {
        	sb.append(' ').append(middleName);
    	}
    	sb.append(' ').append(lastName);
        return sb.toString();
    }
}
