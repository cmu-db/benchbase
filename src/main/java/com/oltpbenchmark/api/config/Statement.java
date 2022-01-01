package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;

public record Statement(@JacksonXmlProperty(localName = "name", isAttribute = true) String name, @JacksonXmlProperty(localName = "", isAttribute = false) String sql) {
}
