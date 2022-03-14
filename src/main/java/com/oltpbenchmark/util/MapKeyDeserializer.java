package com.oltpbenchmark.util;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

class MapKeyDeserializer extends KeyDeserializer {

    private static final ObjectMapper mapper = new ObjectMapper();

    public MapKeyDeserializer() {
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
        return mapper.readValue(key, String.class);
    }
}
