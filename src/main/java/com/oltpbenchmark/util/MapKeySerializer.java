package com.oltpbenchmark.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.io.StringWriter;

public class MapKeySerializer extends JsonSerializer {


    private static final ObjectMapper mapper = new ObjectMapper();

    public MapKeySerializer() {
    }

    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, value.toString());
        gen.writeFieldName(writer.toString());
    }
}
