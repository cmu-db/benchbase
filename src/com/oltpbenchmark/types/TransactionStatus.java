package com.oltpbenchmark.types;

public enum TransactionStatus {
    /**
     * The transaction executed successfully and
     * committed without any errors.
     */
    SUCCESS,
    /**
     * The transaction executed successfully but then was aborted
     * due to the valid user control code.
     * This is not an error.
     */
    USER_ABORTED,
    /**
     * The transaction did not executed due to internal 
     * benchmark state. It should be retried
     */
    RETRY,
    /**
     * The transaction did not executed due to internal 
     * benchmark state. The Worker should retry but select
     * a new random transaction to execute.
     */
    RETRY_DIFFERENT
}
