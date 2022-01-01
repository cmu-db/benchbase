package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Transaction(Class<? extends com.oltpbenchmark.api.Procedure> procedureClass, Integer id, String name, Boolean supplemental, Long preExecutionWait, Long postExecutionWait) {
}
