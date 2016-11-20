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

/**
 * Modified by Djellel for oltpbenchmark
 * difallah@gmail.com
 */

package com.oltpbenchmark.benchmarks.jpab.tests;

import java.util.*;
import java.util.concurrent.atomic.*;

import javax.persistence.EntityManager;
import javax.persistence.OptimisticLockException;
import javax.persistence.Query;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.jpab.objects.TestEntity;



/**
 * Super abstract class of the concrete benchmark test classes.
 */
public abstract class Test {

	//--------------//
	// Action Types //
	//--------------//

	/** Action types for the doAction function */
	public enum ActionType {
		RETRIEVE, UPDATE, DELETE 
	}

	//--------------//
	// Data Members //
	//--------------//

	/** Number of concurrent threads */
	private int threadCount;

	/** Transaction / retrieval size in entities */
	private int batchSize;

	/** Total number of entity objects in the database */
	protected int entityCount;

	/** Count the number of actions performed during a run */
	private final AtomicInteger actionCount = new AtomicInteger();

	/** Inventory of ready to use entity objects for persist */
	private final Stack<TestEntity> entityInventory = new Stack<TestEntity>();

	//--------------//
	// Construction //
	//--------------//

	/**
	 * Constructs a Test instance.
	 */
	public Test() {
	}

	//------------//
	// Properties //
	//------------//

	// General:

	/**
	 * Gets the test name.
	 * 
	 * @return the short name of the test class.
	 */
	public final String getName() {
		return getClass().toString();
	}
	
	// Thread Count:

	/**
	 * Sets the number of concurrent threads for this test.
	 * 
	 * @param threadCount the number of concurrent threads
	 */
	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}

	/**
	 * Gets the number of concurrent threads for this test.
	 * 
	 * @return the number of concurrent threads for this test.
	 */
	public final int getThreadCount() {
		return threadCount;
	}

	// Batch Size:

	/**
	 * Sets the transaction / retrieval size in entities.
	 * 
	 * @param batchSize transaction / retrieval size in entities
	 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	/**
	 * Gets the transaction / retrieval size in entities.
	 * 
	 * @return the transaction / retrieval size in entities.
	 */
	public final int getBatchSize() {
		return batchSize;
	}

	// Entity Count:

	/**
	 * Sets the number of entity objects in the database.
	 * 
	 * @param entityCount number of entity objects in the database
	 */
	public void setEntityCount(int entityCount) {
		this.entityCount = entityCount;
	}

	// Action Count:

	/**
	 * Resets the action count to 0 (at the beginning of the test).
	 */
	public final void resetActionCount() {
		actionCount.set(0);
	}

	/**
	 * Increases the action count.
	 * 
	 * @param actionCount number to be added to the action count
	 */
	protected final void increaseActionCount(int delta) {
		actionCount.addAndGet(delta);
	}

	/**
	 * Gets the number of performed actions (since last reset).
	 * 
	 * @return the number of performed actions (since last reset).
	 */
	public final int getActionCount() {
		return actionCount.get();
	}

	//-----------//
	// Inventory //
	//-----------//

	/**
	 * Builds an inventory of entity objects for persist.
	 * 
	 * @param entityCount size of the inventory (in objects) 
	 */
	public void buildInventory(int entityCount) {
		entityCount /= getGraphSize();
		entityInventory.ensureCapacity(entityCount);
		while (entityCount-- > 0) {
			entityInventory.add(newEntity());
		}
		//Collections.reverse(entityInventory); // LIFO to FIFO
	}

	/**
	 * Clears unused inventory entity objects.  
	 */
	public void clearInventory() {
		entityInventory.clear();
	}

	//--------------//
	// Test Actions //
	//--------------//

	// Persist:

	/**
	 * Persists a batch of entity objects.
	 * 
	 * @param em a connection to the test database
	 */
	public final void persist(EntityManager em) {
		persist(em, batchSize);
	}

	/**
	 * Persists a batch of entity objects.
	 * 
	 * @param em a connection to the test database
	 */
	final void persist(EntityManager em, int batchSize) {
		try {
			em.getTransaction().begin();
			int graphSize = getGraphSize(); // > 1 only in NodeTest
			int operCount = batchSize / graphSize;
			for (int i = 0; i < operCount && !entityInventory.isEmpty(); i++) {
			    TestEntity t=entityInventory.pop();
			    //System.out.println(t.toString());
				em.persist(t);
				increaseActionCount(graphSize);
			}
			em.getTransaction().commit();
		}
		catch (RuntimeException e) {
			if (!isLockException(e))
				throw e; // ignore optimistic lock exceptions
		}
		finally {
			if (em.getTransaction().isActive()) {
				em.getTransaction().rollback();
			}
			em.clear();
		}
	}

	// Retrieve, Update & Remove:

	/**
	 * Performs a retrieve/update/remove action on a batch of entity objects.
	 * 
	 * @param em a connection to the test database
	 * @param action one of RETRIEVE, UPDATE or DELETE
	 */
	public final void doAction(EntityManager em, ActionType action) {
		try {
			// Begin a transaction:
			if (action != ActionType.RETRIEVE) {
				em.getTransaction().begin();
			}

			// Retrieve a batch of entity objects:
			int graphSize = getGraphSize();
			int graphCount = batchSize / graphSize;
			boolean isRandom = action != ActionType.DELETE;
			List<TestEntity> entityList = retireveEntities(em, graphCount, isRandom);

			// Repeat the action on all the entity objects: 
			for (TestEntity entity : entityList) {
				switch (action) {
					case RETRIEVE:
						entity.load();
						break;
					case UPDATE:
						entity.update();
						break;
					case DELETE:
					    //System.out.println("Removed: "+entity.toString());
						em.remove(entity);
						entityCount--;
						break;
					}
			}

			// Commit the transaction:
			increaseActionCount(entityList.size() * graphSize);
			if (action != ActionType.RETRIEVE) {
				em.getTransaction().commit();
			}
		}
		catch (RuntimeException e) {
			if (!isLockException(e))
				throw e; // ignore optimistic lock exceptions
		}
		finally {
			if (em.getTransaction().isActive()) {
				em.getTransaction().rollback();
			}
			em.clear();
		}
	}

	// Query:

	/**
	 * Checks if this test includes queries.
	 * 
	 * @return true - if it includes queries; false - if not.
	 */
	public boolean hasQueries() {
		return true; // overridden by tests with no queries 
	}

    /**
     * Executes a query.
     * 
     * @param em a connection to the test database
     */
    public void query(EntityManager em) {
    	// Prepare a target last name prefix:
    	int prefixLength = 1; // depends on batch size
    	for (int count = entityCount; (count /= 26) > batchSize; ) {
    		prefixLength++;
    	}
    	String prefix = LoaderUtil.randomStr(10);

    	// Execute the query:
        Query query = em.createQuery("SELECT o FROM " +
        	getEntityName() + " o WHERE o.lastName LIKE :pattern");
        query.setParameter("pattern", prefix + "%");
        List<TestEntity> results = query.getResultList();

        // Load the results (expected to be already loaded):
        for (TestEntity entity : results) {
			entity.load();
        }
        increaseActionCount(1);
        em.clear();
    }

	//------------------------//
	// Implementation Methods //
	//------------------------//

	// Test Details:

	/**
	 * Gets the type of the test main entity class.
	 * 
	 * @return the type of the test main entity class.
	 */
	protected abstract Class getEntityClass();

	/**
	 * Gets the unqualified name of the test main entity class.
	 * 
	 * @return the unqualified name of the test main entity class.
	 */
	public abstract String getEntityName();

	/**
	 * Gets the number of reachable objects from every root entity object.
	 * 
	 * @return the number of reachable objects from every root entity object.
	 */
	protected int getGraphSize() {
		return 1; // overridden by NodeTest
	}

	// Entity Operations:

	/**
	 * Creates a new entity object (graph) for storing in the database.
	 * 
	 * @return the new constructed entity object.
	 */
	protected abstract TestEntity newEntity();

	/**
	 * Retrieves entity object roots.
	 * 
	 * @param em a connection to the database
	 * @param count number of requested entity object roots
	 * @param isRandom true - for random retrieval; false - for first returned 
	 * @param the entity object roots.
	 */
	@SuppressWarnings("boxing")
	protected List retireveEntities(
			EntityManager em, int count, boolean isRandom) {
		int maxFirstId = Math.max(entityCount - count, 1);
		int firstId = LoaderUtil.randomNumber(1, maxFirstId, new Random());
		Query query = em.createQuery("SELECT o FROM " + getEntityName() + " o WHERE o.id >= :firstId"); 
		query.setParameter("firstId", firstId);
		query.setMaxResults(count);
		return query.getResultList();
	}

	//----------------//
	// Helper Methods //
	//----------------//

	/**
	 * Checks if a specified exception represents a lock failure.
	 * 
	 * @param e an exception for check
	 * @return true - if it does; false - if it does not.
	 */
	private static boolean isLockException(Throwable e) {
		
		if (e instanceof OptimisticLockException) {
			return true;
		}
		String msg = e.getMessage();
		if (msg != null) {
			msg = msg.toLowerCase();
			if (msg.contains("optimistic") || msg.contains("lock") ||
					msg.contains("timeout")) {
				return true;
			}
		}
		Throwable cause = e.getCause();
		if (cause != null && cause != e) {
			return isLockException(cause);
		}
		return false;
	}
}
