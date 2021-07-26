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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * @author pavlo
 */
public class TestClassUtil extends TestCase {

    private final Class<?> target_class = ArrayList.class;

    public static class MockObject1 {
        public MockObject1(MockObject1 x) {

        }
    }

    public static class MockObject2 {
        public MockObject2(MockObject2 x) {

        }
    }

    public static class MockObject3 extends MockObject2 {
        public MockObject3(MockObject2 x) {
            super(x);
        }
    }

    /**
     * testGetConstructor
     */
    public void testGetConstructor() throws Exception {
        Class<?>[] targets = {
                MockObject1.class,
                MockObject2.class,
        };
        Class<?>[] params = {
                MockObject1.class
        };

        for (Class<?> targetClass : targets) {
            Constructor<?> c = ClassUtil.getConstructor(targetClass, params);
            assertNotNull(c);
        } // FOR
    }


    /**
     * testGetSuperClasses
     */
    public void testGetSuperClasses() {
        Class<?>[] expected = {
                target_class,
                AbstractList.class,
                AbstractCollection.class,
                Object.class,
        };
        List<Class<?>> results = ClassUtil.getSuperClasses(target_class);
        // System.err.println(target_class + " => " + results);
        assert (!results.isEmpty());
        assertEquals(expected.length, results.size());

        for (Class<?> e : expected) {
            assert (results.contains(e));
        } // FOR
    }


    /**
     * testGetSuperClassesCatalogType
     */
    public void testGetSuperClassesCatalogType() {
        Class<?>[] expected = {
                MockObject3.class,
                MockObject2.class,
                Object.class,
        };
        List<Class<?>> results = ClassUtil.getSuperClasses(MockObject3.class);
        assert (!results.isEmpty());
        assertEquals(expected.length, results.size());

        for (Class<?> e : expected) {
            assert (results.contains(e));
        } // FOR
    }

    /**
     * GetInterfaces
     */
    public void testGetInterfaces() {
        Class<?>[] expected = {
                Serializable.class,
                Cloneable.class,
                Iterable.class,
                Collection.class,
                List.class,
                RandomAccess.class,
        };
        Collection<Class<?>> results = ClassUtil.getInterfaces(target_class);
        // System.err.println(target_class + " => " + results);
        assert (!results.isEmpty());
        assertEquals(expected.length, results.size());

        for (Class<?> e : expected) {
            assert (results.contains(e));
        } // FOR
    }
}
