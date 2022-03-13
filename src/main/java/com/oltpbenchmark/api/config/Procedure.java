package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.List;

public record Procedure(@JacksonXmlProperty(localName = "procedureClass", isAttribute = true) Class<? extends com.oltpbenchmark.api.Procedure> procedureClass,
                        List<Statement> statements) {
}
