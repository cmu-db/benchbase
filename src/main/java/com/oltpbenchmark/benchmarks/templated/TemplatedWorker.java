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
package com.oltpbenchmark.benchmarks.templated;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.templated.procedures.GenericQuery;
import com.oltpbenchmark.benchmarks.templated.util.TraceTransactionGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public final class TemplatedWorker extends Worker<TemplatedBenchmark> {

  protected final Map<Class<? extends Procedure>, TraceTransactionGenerator> generators;

  public TemplatedWorker(
      TemplatedBenchmark benchmarkModule,
      int id,
      Map<Class<? extends Procedure>, TraceTransactionGenerator> generators) {
    super(benchmarkModule, id);
    this.rng().setSeed(benchmarkModule.getWorkloadConfiguration().getRandomSeed());
    this.generators = generators;
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType nextTransaction)
      throws UserAbortException, SQLException {
    try {
      Class<? extends Procedure> clazz = nextTransaction.getProcedureClass();
      GenericQuery proc = (GenericQuery) this.getProcedure(clazz);
      if (!generators.get(clazz).isEmpty()) {
        // If there is a generator available use it to create a
        // parameter binding.
        TraceTransactionGenerator generator = generators.get(clazz);
        proc.run(conn, generator.nextTransaction().getParams());
      } else {
        // If the generator has no transactions, there are no parameters.
        proc.run(conn);
      }

    } catch (ClassCastException e) {
      throw new RuntimeException(e);
    }

    return (TransactionStatus.SUCCESS);
  }
}
