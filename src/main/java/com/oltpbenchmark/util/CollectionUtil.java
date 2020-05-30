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

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.commons.lang3.NotImplementedException;

import java.util.*;

/**
 * @author pavlo
 */
public abstract class CollectionUtil {

    private static final Random RANDOM = new Random();

    /**
     * Put all of the elements in items into the given array
     * This assumes that the array has been pre-allocated
     *
     * @param <T>
     * @param items
     * @param array
     */
    public static <T> void toArray(Collection<T> items, Object[] array, boolean convert_to_primitive) {


        int i = 0;
        for (T t : items) {
            if (convert_to_primitive) {
                if (t instanceof Long) {
                    array[i] = t;
                } else if (t instanceof Integer) {
                    array[i] = t;
                } else if (t instanceof Double) {
                    array[i] = t;
                } else if (t instanceof Boolean) {
                    array[i] = t;
                }
            } else {
                array[i] = t;
            }
        }
        return;
    }

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
     * Put all of the values of an Enumeration into a new List
     *
     * @param <T>
     * @param e
     * @return
     */
    public static <T> List<T> list(Enumeration<T> e) {
        return (list(iterable(e)));
    }

    /**
     * Put all of the values of an Iterable into a new List
     *
     * @param <T>
     * @param it
     * @return
     */
    public static <T> List<T> list(Iterable<T> it) {
        return (list(it.iterator()));
    }

    /**
     * Put all the values of an Iterator into a Set
     *
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Set<T> set(Iterator<T> it) {
        Set<T> set = new HashSet<>();
        CollectionUtil.addAll(set, it);
        return (set);
    }

    /**
     * Put all of the values of an Iterable into a new Set
     *
     * @param <T>
     * @param it
     * @return
     */
    public static <T> Set<T> set(Iterable<T> it) {
        return (set(it.iterator()));
    }

    /**
     * Return a random value from the given Collection
     *
     * @param <T>
     * @param items
     */
    public static <T> T random(Collection<T> items) {
        return (CollectionUtil.random(items, RANDOM));
    }

    /**
     * Return a random value from the given Collection
     *
     * @param <T>
     * @param items
     * @param rand
     * @return
     */
    public static <T> T random(Collection<T> items, Random rand) {
        int idx = rand.nextInt(items.size());
        return (CollectionUtil.get(items, idx));
    }

    /**
     * Return a random value from the given Iterable
     *
     * @param <T>
     * @param it
     * @return
     */
    public static <T> T random(Iterable<T> it) {
        return (CollectionUtil.random(it, RANDOM));
    }

    /**
     * Return a random value from the given Iterable
     *
     * @param <T>
     * @param it
     * @param rand
     * @return
     */
    public static <T> T random(Iterable<T> it, Random rand) {
        List<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }
        return (CollectionUtil.random(list, rand));
    }


    /**
     * Add all the items in the array to a Collection
     *
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, T... items) {
        data.addAll(Arrays.asList(items));
        return (data);
    }

    /**
     * Add all the items in the Enumeration into a Collection
     *
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Enumeration<T> items) {
        while (items.hasMoreElements()) {
            data.add(items.nextElement());
        }
        return (data);
    }

    /**
     * Add all of the items from the Iterable into the given collection
     *
     * @param <T>
     * @param data
     * @param items
     */
    public static <T> Collection<T> addAll(Collection<T> data, Iterable<T> items) {
        return (CollectionUtil.addAll(data, items.iterator()));
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
     * Return the first item in a Iterator
     *
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Iterator<T> items) {
        return (items.hasNext() ? items.next() : null);
    }

    /**
     * Returns the first item in an Enumeration
     *
     * @param <T>
     * @param items
     * @return
     */
    public static <T> T first(Enumeration<T> items) {
        return (items.hasMoreElements() ? items.nextElement() : null);
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

    /**
     * Wrap an Iterable around an Enumeration
     *
     * @param <T>
     * @param e
     * @return
     */
    public static <T> Iterable<T> iterable(final Enumeration<T> e) {
        return (new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return (e.hasMoreElements());
                    }

                    @Override
                    public T next() {
                        return (e.nextElement());
                    }

                    @Override
                    public void remove() {
                        throw new NotImplementedException("remove not implemented");
                    }
                };
            }
        });
    }

    public static <T> T pop(Collection<T> items) {
        T t = CollectionUtil.first(items);
        if (t != null) {
            boolean ret = items.remove(t);

        }
        return (t);
    }
}
