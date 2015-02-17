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

package com.oltpbenchmark.benchmarks.jpab.tests;

import javax.persistence.EntityManager;

import com.oltpbenchmark.benchmarks.jpab.objects.IndexedPerson;
import com.oltpbenchmark.benchmarks.jpab.objects.TestEntity;


/**
 * Tests using simple Person entity objects with an index.
 */
public class IndexTest extends Test {
    
    /**
     * Gets the type of the benchmark main entity class.
     * 
     * @return the type of the benchmark main entity class.
     */
    @Override
    protected Class getEntityClass() {
        return IndexedPerson.class;
    }

	/**
	 * Creates a new entity object for storing in the database.
	 * 
	 * @return the new constructed entity object.
	 */
    @Override
    protected TestEntity newEntity() {
        return new IndexedPerson(this);
    }

	@Override
	public String getEntityName() {
		// TODO Auto-generated method stub
		return "IndexedPerson";
	}
	
	public void run(EntityManager em) {	
		this.persist(em);
		this.doAction(em, Test.ActionType.RETRIEVE);
		if (this.hasQueries()) {
			this.query(em);
		}
		this.doAction(em, Test.ActionType.UPDATE);
		this.doAction(em, Test.ActionType.DELETE);
	}
}
