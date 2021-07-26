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

/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.oltpbenchmark.util;

import java.util.Objects;

/**
 * Class representing a pair of generic-ized types. Supports equality, hashing
 * and all that other nice Java stuff. Based on STL's pair class in C++.
 */
public class Pair<T, U> implements Comparable<Pair<T, U>> {

    public final T first;
    public final U second;
    private final transient Integer hash;

    public Pair(T first, U second, boolean precomputeHash) {
        this.first = first;
        this.second = second;
        hash = (precomputeHash ? this.computeHashCode() : null);
    }

    public Pair(T first, U second) {
        this(first, second, true);
    }

    private int computeHashCode() {
        return (first == null ? 0 : first.hashCode() * 31) +
                (second == null ? 0 : second.hashCode());
    }

    public int hashCode() {
        if (hash != null) {
            return (hash);
        }
        return (this.computeHashCode());
    }

    public String toString() {
        return String.format("<%s, %s>", first, second);
    }

    @Override
    public int compareTo(Pair<T, U> other) {
        return (other.hash - this.hash);
    }

    @SuppressWarnings("unchecked")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || !(o instanceof Pair)) {
            return false;
        }


        Pair<T, U> other = (Pair<T, U>) o;

        return (Objects.equals(first, other.first))
                && (Objects.equals(second, other.second));
    }

    /**
     * Convenience class method for constructing pairs using Java's generic type
     * inference.
     */
    public static <T, U> Pair<T, U> of(T x, U y) {
        return new Pair<>(x, y);
    }
}
