/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.collections15.set.ListOrderedSet;

/**
 * @author pavlo
 */
public class TestCollectionUtil extends TestCase {
    
    private final Random rand = new Random();
    
    /**
     * testIterableEnumeration
     */
    public void testIterableEnumeration() {
        final int size = 10;
        Enumeration<Integer> e = new Enumeration<Integer>() {
            int ctr = 0;
            @Override
            public Integer nextElement() {
                return (ctr++);
            }
            @Override
            public boolean hasMoreElements() {
                return (ctr < size);
            }
        };
        
        List<Integer> found = new ArrayList<Integer>();
        for (Integer i : CollectionUtil.iterable(e))
            found.add(i);
        assertEquals(size, found.size());
    }
    
    /**
     * testAddAll
     */
    public void testAddAll() {
        int cnt = rand.nextInt(50) + 1; 
        List<Integer> l = new ArrayList<Integer>();
        Integer a[] = new Integer[cnt];
        for (int i = 0; i < cnt; i++) {
            int next = rand.nextInt(); 
            l.add(next);
            a[i] = next;
        } // FOR
        
        Collection<Integer> c = CollectionUtil.addAll(new HashSet<Integer>(), l);
        assertEquals(l.size(), c.size());
        assert(c.containsAll(l));
        
        c = CollectionUtil.addAll(new HashSet<Integer>(), a);
        assertEquals(l.size(), c.size());
        assert(c.containsAll(l));
    }
    
    /**
     * testGetGreatest
     */
    public void testGetGreatest() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 4);
        map.put("d", 3);
        String key = CollectionUtil.getGreatest(map);
        assertEquals("c", key);
    }
    
    /**
     * testGetFirst
     */
    public void testGetFirst() {
        List<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        String key = CollectionUtil.first(list);
        assertEquals("a", key);
    }
    
    /**
     * testPop
     */
    @SuppressWarnings("unchecked")
    public void testPop() {
        String expected[] = new String[11];
        RandomGenerator rng = new RandomGenerator(0);
        for (int i = 0; i < expected.length; i++) {
            expected[i] = rng.astring(1, 32);
        } // FOR
        
        Collection<String> collections[] = new Collection[] {
            CollectionUtil.addAll(new ListOrderedSet<String>(), expected),
            CollectionUtil.addAll(new HashSet<String>(), expected),
            CollectionUtil.addAll(new ArrayList<String>(), expected),
        };
        for (Collection<String> c : collections) {
            assertNotNull(c);
            assertEquals(c.getClass().getSimpleName(), expected.length, c.size());
            String pop = CollectionUtil.pop(c);
            assertNotNull(c.getClass().getSimpleName(), pop);
            assertEquals(c.getClass().getSimpleName(), expected.length-1, c.size());
            assertFalse(c.getClass().getSimpleName(), c.contains(pop));
            
            if (c instanceof List || c instanceof ListOrderedSet) {
                assertEquals(c.getClass().getSimpleName(), expected[0], pop);
            }
        } // FOR
    }
}
