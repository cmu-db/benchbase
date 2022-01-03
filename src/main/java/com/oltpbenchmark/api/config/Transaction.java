package com.oltpbenchmark.api.config;

public record Transaction(Class<? extends com.oltpbenchmark.api.Procedure> procedureClass,
                          String name,
                          Boolean supplemental,
                          Long preExecutionWait,
                          Long postExecutionWait) {
}
