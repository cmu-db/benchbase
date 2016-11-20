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

import java.util.*;
import java.util.concurrent.atomic.*;

import javax.persistence.*;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.jpab.objects.Node;
import com.oltpbenchmark.benchmarks.jpab.objects.TestEntity;


/**
 * Tests using graphs (trees) of entity objects.  
 */
public class NodeTest extends Test {

	//-----------//
    // Constants //
	//-----------//

    /** The size of a single tree in nodes (entity objects) */
    private static final int GRAPH_SIZE = 100;
    
    // Data Members:

    /** Last allocated tree ID - for generating primary keys */
    private final AtomicInteger lastTreeId = new AtomicInteger(0); 

    // Test Methods:

	/**
	 * Checks if this test includes queries.
	 * 
	 * @return true - if it includes queries; false - if not.
	 */
	@Override
	public boolean hasQueries() {
		return false; 
	}

    /**
     * Gets the type of the benchmark main entity class.
     * 
     * @return the type of the benchmark main entity class.
     */
    @Override
    protected Class getEntityClass() {
        return Node.class; 
    }

    /**
     * Gets the number of reachable objects from every root entity object. 
     * 
     * @return the number of reachable objects from every root entity object.
     */
    @Override
    protected int getGraphSize() {
        return GRAPH_SIZE;
    }

	/**
	 * Creates a new entity object (graph) for storing in the database.
	 * 
	 * @return the new constructed entity object.
	 */
    @Override
    protected TestEntity newEntity() {
        int treeId = lastTreeId.incrementAndGet();
        Node[] nodes = new Node[GRAPH_SIZE + 1];
        for (int nodeIx = 1; nodeIx <= GRAPH_SIZE; nodeIx++)
        {
            Node node = new Node(treeId * GRAPH_SIZE + nodeIx);
            nodes[nodeIx] = node;
            int parentIx = nodeIx >> 1;
            if (parentIx > 0) {
                if ((nodeIx % 2) == 0) {
                    nodes[parentIx].setChild1(node);
                }
                else {
                    nodes[parentIx].setChild2(node);
                }
            }
        }
        return nodes[1];
    }

	/**
	 * Retrieves entity object roots.
	 * 
	 * @param em a connection to the database
	 * @param count number of requested entity object roots
	 * @param isRandom true - for random retrieval; false - for first returned 
	 * @param the entity object roots.
	 */
	@Override
	@SuppressWarnings("boxing")
	protected List retireveEntities(EntityManager em, int count,
			boolean isRandom) {
		int graphSize = getGraphSize();
		Query query = em.createQuery("SELECT o FROM " + getEntityName() +
			" o WHERE MOD(o.id, :graphSize) = 1 AND o.id >= :firstId");
		query.setParameter("graphSize", graphSize);
		if (isRandom) {		
			int maxFirstId = Math.max(entityCount - count * graphSize, 1);
			int firstId = LoaderUtil.randomNumber(1, maxFirstId, new Random());
			query.setParameter("firstId", firstId);
		}
		else {
			query.setParameter("firstId", 0);
		}
		query.setMaxResults(count);
		return query.getResultList();
	}

	@Override
	public String getEntityName() {
		// TODO Auto-generated method stub
		return "Node";
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