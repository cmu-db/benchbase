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


import java.util.Objects;

public class TransactionType implements Comparable<TransactionType> {

    public static class Invalid extends Procedure {
    }

    public static final int INVALID_ID = 0;
    public static final TransactionType INVALID = new TransactionType(Invalid.class, INVALID_ID);

    private final Class<? extends Procedure> procClass;
    private final int id;

    protected TransactionType(Class<? extends Procedure> procClass, int id) {
        this.procClass = procClass;
        this.id = id;
    }

    public Class<? extends Procedure> getProcedureClass() {
        return (this.procClass);
    }

    public String getName() {
        return this.procClass.getSimpleName();
    }

    public int getId() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionType that = (TransactionType) o;
        return id == that.id &&
                Objects.equals(procClass, that.procClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(procClass, id);
    }

    @Override
    public int compareTo(TransactionType o) {
        return (this.id - o.id);
    }

    @Override
    public String toString() {
        return String.format("%s/%02d", this.procClass.getName(), this.id);
    }

}
