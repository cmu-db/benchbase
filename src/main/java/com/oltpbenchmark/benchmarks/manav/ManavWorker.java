/*
 * Copyright 2024 by BenchBase Project
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
 */

package com.oltpbenchmark.benchmarks.manav;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.manav.procedures.InsertRecord;
import com.oltpbenchmark.types.TransactionStatus;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manav Benchmark Worker Performs INSERT-only transactions on the logs table */
public final class ManavWorker extends Worker<ManavBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(ManavWorker.class);

  private final InsertRecord procInsertRecord;
  private final Random messageRandom;
  private long transactionCounter = 0;

  public ManavWorker(ManavBenchmark benchmarkModule, int id) {
    super(benchmarkModule, id);

    LOG.info("Initializing ManavWorker with ID: {}", id);

    // Cache the procedure to avoid hashmap lookup for each transaction
    this.procInsertRecord = this.getProcedure(InsertRecord.class);

    // Initialize random generator for message selection
    this.messageRandom = new Random(this.rng().nextLong());

    LOG.info("ManavWorker {} initialization completed successfully", id);
  }

  /** Generate a random log message */
  private String generateLogMessage() {
    String baseMessage;

    // 50% chance to use a predefined message, 50% chance to generate a unique one
    if (messageRandom.nextBoolean()) {
      // Use a predefined message
      baseMessage =
          ManavConstants.SAMPLE_MESSAGES[
              messageRandom.nextInt(ManavConstants.SAMPLE_MESSAGES.length)];
    } else {
      // Generate a unique message with transaction counter and timestamp
      baseMessage =
          ManavConstants.MESSAGE_PREFIX
              + "Worker-"
              + getId()
              + " Transaction-"
              + transactionCounter
              + " Time-"
              + System.currentTimeMillis();
    }

    // Ensure message doesn't exceed maximum length
    if (baseMessage.length() > ManavConstants.MAX_MESSAGE_LENGTH) {
      baseMessage = baseMessage.substring(0, ManavConstants.MAX_MESSAGE_LENGTH - 3) + "...";
    }

    return baseMessage;
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType txnType)
      throws UserAbortException, SQLException {

    transactionCounter++;

    LOG.debug(
        "Worker {} starting transaction #{} of type: {}",
        getId(),
        transactionCounter,
        txnType.getName());

    Class<? extends Procedure> procClass = txnType.getProcedureClass();

    // We only have one transaction type: InsertRecord
    if (procClass.equals(InsertRecord.class)) {
      String logMessage = generateLogMessage();

      LOG.info(
          "Worker {} executing INSERT transaction #{} with message: '{}'",
          getId(),
          transactionCounter,
          logMessage);

      try {
        int rowsInserted = this.procInsertRecord.run(conn, logMessage, getId());

        if (rowsInserted == 1) {
          LOG.info(
              "Worker {} successfully completed transaction #{} - inserted {} row(s)",
              getId(),
              transactionCounter,
              rowsInserted);
          return TransactionStatus.SUCCESS;
        } else {
          LOG.warn(
              "Worker {} transaction #{} completed but inserted unexpected number of rows: {}",
              getId(),
              transactionCounter,
              rowsInserted);
          return TransactionStatus.SUCCESS; // Still consider it success
        }

      } catch (SQLException e) {
        LOG.error(
            "Worker {} transaction #{} failed with SQLException: {}",
            getId(),
            transactionCounter,
            e.getMessage(),
            e);
        throw e;
      }

    } else {
      String errorMsg =
          "Worker " + getId() + " received unknown transaction type: " + procClass.getName();
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
  }

  @Override
  public String toString() {
    return String.format("ManavWorker<%03d>[Transactions: %d]", getId(), transactionCounter);
  }
}
