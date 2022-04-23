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

package com.oltpbenchmark.api;

import org.apache.commons.collections4.map.ListOrderedMap;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class TransactionTypes implements Collection<TransactionType> {

    private final ListOrderedMap<String, TransactionType> types = new ListOrderedMap<>();

    public TransactionTypes(List<TransactionType> transactiontypes) {
        transactiontypes.sort(TransactionType::compareTo);
        for (TransactionType tt : transactiontypes) {
            String key = tt.getName().toUpperCase();
            this.types.put(key, tt);
        }
    }

    public TransactionType getType(String procName) {
        return (this.types.get(procName.toUpperCase()));
    }

    public TransactionType getType(Class<? extends Procedure> procClass) {
        return (this.getType(procClass.getSimpleName()));
    }

    public TransactionType getType(int id) {
        return (this.types.getValue(id));
    }

    @Override
    public String toString() {
        return this.types.values().toString();
    }

    @Override
    public boolean add(TransactionType tt) {
        String key = tt.getName().toUpperCase();
        this.types.put(key, tt);
        return (true);
    }

    @Override
    public boolean addAll(Collection<? extends TransactionType> c) {
        return false;
    }

    @Override
    public void clear() {
        this.types.clear();
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return (this.types.values().containsAll(c));
    }

    @Override
    public boolean isEmpty() {
        return (this.types.isEmpty());
    }

    @Override
    public Iterator<TransactionType> iterator() {
        return (this.types.values().iterator());
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public int size() {
        return (this.types.size());
    }

    @Override
    public Object[] toArray() {
        return (this.types.values().toArray());
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return (this.types.values().toArray(a));
    }

}
