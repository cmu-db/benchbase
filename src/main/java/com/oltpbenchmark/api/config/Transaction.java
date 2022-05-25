package com.oltpbenchmark.api.config;

public record Transaction(Class<? extends com.oltpbenchmark.api.Procedure> procedureClass,
                          Boolean supplemental,
                          Long preExecutionWait,
                          Long postExecutionWait) {
}
