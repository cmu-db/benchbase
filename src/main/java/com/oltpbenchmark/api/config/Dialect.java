package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oltpbenchmark.types.DatabaseType;

import java.util.List;

@JacksonXmlRootElement(localName = "dialect")
public record Dialect(@JacksonXmlProperty(localName = "type", isAttribute = true) DatabaseType databaseType, @JacksonXmlProperty(localName = "procedures", isAttribute = false) List<Procedure> procedures) {
}
