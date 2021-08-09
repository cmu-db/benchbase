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


package com.oltpbenchmark.util;

import junit.framework.TestCase;
import org.apache.commons.collections4.set.ListOrderedSet;

import java.util.*;

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
        for (Integer i : CollectionUtil.iterable(e.asIterator()))
            found.add(i);
        assertEquals(size, found.size());
    }

    /**
     * testAddAll
     */
    public void testAddAll() {
        int cnt = rand.nextInt(50) + 1;
        List<Integer> l = new ArrayList<Integer>();
        Integer[] a = new Integer[cnt];
        for (int i = 0; i < cnt; i++) {
            int next = rand.nextInt();
            l.add(next);
            a[i] = next;
        } // FOR

        Collection<Integer> c = CollectionUtil.addAll(new HashSet<Integer>(), l.iterator());
        assertEquals(l.size(), c.size());
        assert (c.containsAll(l));

        c = CollectionUtil.addAll(new HashSet<Integer>(), a);
        assertEquals(l.size(), c.size());
        assert (c.containsAll(l));
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
        String[] expected = new String[11];
        RandomGenerator rng = new RandomGenerator(0);
        for (int i = 0; i < expected.length; i++) {
            expected[i] = rng.astring(1, 32);
        } // FOR

        Collection<String>[] collections = new Collection[]{
                CollectionUtil.addAll(new ListOrderedSet<String>(), expected),
                CollectionUtil.addAll(new HashSet<String>(), expected),
                CollectionUtil.addAll(new ArrayList<String>(), expected),
        };
        for (Collection<String> c : collections) {
            assertNotNull(c);
            assertEquals(c.getClass().getSimpleName(), expected.length, c.size());
            String pop = CollectionUtil.pop(c);
            assertNotNull(c.getClass().getSimpleName(), pop);
            assertEquals(c.getClass().getSimpleName(), expected.length - 1, c.size());
            assertFalse(c.getClass().getSimpleName(), c.contains(pop));

            if (c instanceof List || c instanceof ListOrderedSet) {
                assertEquals(c.getClass().getSimpleName(), expected[0], pop);
            }
        } // FOR
    }
}
