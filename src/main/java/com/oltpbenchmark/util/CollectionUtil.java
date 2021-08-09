/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.util;

import org.apache.commons.collections4.set.ListOrderedSet;

import java.util.*;

/**
 * @author pavlo
 */
public abstract class CollectionUtil {

    /**
     * Put all the values of an Iterator into a List
     *
     * @param <T>
     * @param it
     * @return
     */
    public static <T> List<T> list(Iterator<T> it) {
        List<T> list = new ArrayList<>();
        CollectionUtil.addAll(list, it);
        return (list);
    }


    /**
     * Add all the items in the array to a Collection
     *
     * @param <T>
     * @param data
     * @param items
     */
    @SuppressWarnings("unchecked")
    public static <T> Collection<T> addAll(Collection<T> data, T... items) {
        data.addAll(Arrays.asList(items));
        return (data);
    }

    /**
     * Add all of the items from the Iterator into the given collection
     *
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Iterator<T> items) {
        while (items.hasNext()) {
            data.add(items.next());
        }
        return (data);
    }

    /**
     * @param <T>
     * @param <U>
     * @param map
     * @return
     */
    public static <T, U extends Comparable<U>> T getGreatest(Map<T, U> map) {
        T max_key = null;
        U max_value = null;
        for (Map.Entry<T, U> e : map.entrySet()) {
            T key = e.getKey();
            U value = e.getValue();
            if (max_value == null || value.compareTo(max_value) > 0) {
                max_value = value;
                max_key = key;
            }
        } // FOR
        return (max_key);
    }

    /**
     * Return the first item in a Iterable
     *
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Iterable<T> items) {
        return (CollectionUtil.get(items, 0));
    }

    /**
     * Return the ith element of a set. Super lame
     *
     * @param <T>
     * @param items
     * @param idx
     * @return
     */
    public static <T> T get(Iterable<T> items, int idx) {
        if (items instanceof AbstractList<?>) {
            return ((AbstractList<T>) items).get(idx);
        } else if (items instanceof ListOrderedSet<?>) {
            return ((ListOrderedSet<T>) items).get(idx);
        }
        int ctr = 0;
        for (T t : items) {
            if (ctr++ == idx) {
                return (t);
            }
        }
        return (null);
    }

    /**
     * Return the last item in an Iterable
     *
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T last(Iterable<T> items) {
        T last = null;
        if (items instanceof AbstractList<?>) {
            AbstractList<T> list = (AbstractList<T>) items;
            last = (list.isEmpty() ? null : list.get(list.size() - 1));
        } else {
            for (T t : items) {
                last = t;
            }
        }
        return (last);
    }

    /**
     * Return the last item in an array
     *
     * @param <T>
     * @param items
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T last(T... items) {
        if (items != null && items.length > 0) {
            return (items[items.length - 1]);
        }
        return (null);
    }

    /**
     * Wrap an Iterable around an Iterator
     *
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Iterable<T> iterable(final Iterator<T> it) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return (it);
            }
        });
    }

    public static <T> T pop(Collection<T> items) {
        T t = CollectionUtil.first(items);
        if (t != null) {
            items.remove(t);

        }
        return (t);
    }
}
