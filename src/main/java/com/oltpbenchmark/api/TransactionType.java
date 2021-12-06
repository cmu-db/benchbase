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
    public static final TransactionType INVALID = new TransactionType(Invalid.class, INVALID_ID, false, 0, 0);

    private final Class<? extends Procedure> procedureClass;
    private final int id;
    private final boolean supplemental;
    private final long preExecutionWait;
    private final long postExecutionWait;

    protected TransactionType(Class<? extends Procedure> procedureClass, int id, boolean supplemental, long preExecutionWait, long postExecutionWait) {
        this.procedureClass = procedureClass;
        this.id = id;
        this.supplemental = supplemental;
        this.preExecutionWait = preExecutionWait;
        this.postExecutionWait = postExecutionWait;
    }

    public Class<? extends Procedure> getProcedureClass() {
        return (this.procedureClass);
    }

    public String getName() {
        return this.procedureClass.getSimpleName();
    }

    public int getId() {
        return this.id;
    }

    public boolean isSupplemental() {
        return this.supplemental;
    }

    public long getPreExecutionWait() {
        return preExecutionWait;
    }

    public long getPostExecutionWait() {
        return postExecutionWait;
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
        return id == that.id && supplemental == that.supplemental && preExecutionWait == that.preExecutionWait && postExecutionWait == that.postExecutionWait && procedureClass.equals(that.procedureClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(procedureClass, id, supplemental, preExecutionWait, postExecutionWait);
    }

    @Override
    public int compareTo(TransactionType o) {
        return (this.id - o.id);
    }

    @Override
    public String toString() {
        return String.format("%s/%02d", this.procedureClass.getName(), this.id);
    }

}
