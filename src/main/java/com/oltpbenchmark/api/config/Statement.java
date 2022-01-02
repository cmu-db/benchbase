package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public record Statement(@JacksonXmlProperty(localName = "name", isAttribute = true) String name,
                        @JacksonXmlProperty(localName = "", isAttribute = false) String sql) {
}
