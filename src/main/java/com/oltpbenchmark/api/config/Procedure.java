package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.List;

public record Procedure(@JacksonXmlProperty(localName = "name", isAttribute = true) String name,
                        List<Statement> statements) {
}
