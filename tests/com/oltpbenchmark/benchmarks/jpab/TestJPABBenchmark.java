/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.jpab;

import com.oltpbenchmark.api.AbstractTestBenchmarkModule;
import com.oltpbenchmark.benchmarks.jpab.tests.BasicTest;
import com.oltpbenchmark.benchmarks.jpab.tests.CollectionTest;
import com.oltpbenchmark.benchmarks.jpab.tests.ExtTest;
import com.oltpbenchmark.benchmarks.jpab.tests.IndexTest;
import com.oltpbenchmark.benchmarks.jpab.tests.NodeTest;

public class TestJPABBenchmark extends AbstractTestBenchmarkModule<JPABBenchmark> {
	
    public static final Class<?> PROC_CLASSES[] = {
        BasicTest.class,
        CollectionTest.class,
        ExtTest.class,
        IndexTest.class,
        NodeTest.class
    };
    
	@Override
	protected void setUp() throws Exception {
		super.setUp(JPABBenchmark.class, PROC_CLASSES);
	}

}
