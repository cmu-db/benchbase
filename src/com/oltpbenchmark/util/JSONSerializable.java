package com.oltpbenchmark.util;

import java.io.IOException;

import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONObject;
import com.oltpbenchmark.util.json.JSONString;
import com.oltpbenchmark.util.json.JSONStringer;

public interface JSONSerializable extends JSONString {
    public void save(String output_path) throws IOException;
    public void load(String input_path) throws IOException;
    public void toJSON(JSONStringer stringer) throws JSONException;
    public void fromJSON(JSONObject json_object) throws JSONException;
}
