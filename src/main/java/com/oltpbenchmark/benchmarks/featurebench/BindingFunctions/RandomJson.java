package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

import org.json.JSONObject;


public class RandomJson {

    protected int fields;
    protected int nestedness ;
    protected Object valueType;
    protected int valueLength;


    public RandomJson(int fields, int nestedness, Object valueType, int valueLength) {
        this.fields = fields;
        this.nestedness = nestedness;
        this.valueType = valueType;
        this.valueLength = valueLength;
    }

    public String getJsonAsString() {
        JSONObject outer = new JSONObject();
        for (int i = 0; i < fields; i++) {
           // JSONObject inner = new JSONObject();
            if (valueType.getClass().equals(String.class)) {
                outer.put(Integer.toString(i), new RandomStringAlphabets(valueLength).getAlphaString());
            } else if (valueType.getClass().equals(Integer.class)) {
                outer.put(Integer.toString(i), new RandomStringNumeric(valueLength).getNumericString());
            } else if (valueType.getClass().equals(Boolean.class)) {
                outer.put(Integer.toString(i), new RandomBoolean().getRandomBoolean());
            }
        }

        return outer.toString();
    }
/*
    public static void main(String args[]) {
        RandomJson js = new RandomJson(4, 1, "a", 9);

        String json = js.getJsonAsString();
        System.out.println(json);

    }*/


}
