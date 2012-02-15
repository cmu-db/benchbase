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



/**
 * A simple binary tree node entity class.
 */
@Entity
public class Node implements TestEntity {
    
	// Fields:

	@Id Integer id;
    
    @Basic int changeCount;

    @ManyToOne(cascade=CascadeType.ALL, fetch=FetchType.EAGER)
    private Node child1;

    @ManyToOne(cascade=CascadeType.ALL, fetch=FetchType.EAGER)
    private Node child2;

	// Constructors:

    public Node() {
    }
    
    public Node(int id) {
        this.id = Integer.valueOf(id);
    }

	// Methods:

    public void setChild1(Node child1) {
        this.child1 = child1;
    }

    public Node getChild1() {
        return child1;
    }

    public void setChild2(Node child2) {
        this.child2 = child2;
    }

    public Node getChild2() {
        return child2;
    }

    public void load() {
        if (child1 != null) {
            child1.load();
        }
        if (child2 != null) {
            child2.load();
        }
    }

    public void update() {
        changeCount++;
        if (child1 != null) {
            child1.update();
        }
        if (child2 != null) {
            child2.update();
        }
    }

    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
