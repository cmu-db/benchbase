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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.*;

import junit.framework.TestCase;

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
        Class<?> targets[] = {
            MockObject1.class,
            MockObject2.class,
        };
        Class<?> params[] = {
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
        Class<?> expected[] = {
            target_class,
            AbstractList.class,
            AbstractCollection.class,
            Object.class,
        };
        List<Class<?>> results = ClassUtil.getSuperClasses(target_class);
        // System.err.println(target_class + " => " + results);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
    

    /**
     * testGetSuperClassesCatalogType
     */
    public void testGetSuperClassesCatalogType() {
        Class<?> expected[] = {
            MockObject3.class,
            MockObject2.class,
            Object.class,
        };
        List<Class<?>> results = ClassUtil.getSuperClasses(MockObject3.class);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
    
    /**
     * GetInterfaces
     */
    public void testGetInterfaces() {
        Class<?> expected[] = {
            Serializable.class,
            Cloneable.class,
            Iterable.class,
            Collection.class,
            List.class,
            RandomAccess.class,
        };
        Collection<Class<?>> results = ClassUtil.getInterfaces(target_class);
        // System.err.println(target_class + " => " + results);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
}
