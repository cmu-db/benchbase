package com.oltpbenchmark.benchmarks.featurebench.workerhelpers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oltpbenchmark.benchmarks.featurebench.helpers.UtilToMethod;
import java.util.regex.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Query {
    @JsonProperty("query")
    public String query;
    public int count = 1;
    public int explainPlanRCValidateCount = -1;
    public int pattern_count = -1;
    public List<UtilToMethod> baseUtils;
    public boolean isSelectQuery = false;
    public boolean isUpdateQuery = false;

    public List<UtilToMethod> getBaseUtils() {
        return baseUtils;
    }

    public void setBaseUtils(List<UtilToMethod> baseUtils) {
        if (this.pattern_count != -1) {
            this.baseUtils = repeatList(baseUtils, this.pattern_count);
        } else {
            this.baseUtils = baseUtils;
        }
    }

    public String getQuery() {
        return query;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setExplainPlanRCValidateCount(int explainPlanRCValidateCount) {
        this.explainPlanRCValidateCount = explainPlanRCValidateCount;
    }

    public int getExplainPlanRCValidateCount() {
        return explainPlanRCValidateCount;
    }

    public void setQuery(String query) {
        this.query = processQuery(query);
    }

    public boolean isSelectQuery() {
        return isSelectQuery;
    }

    public void setSelectQuery(boolean selectQuery) {
        isSelectQuery = selectQuery;
    }

    public boolean isUpdateQuery() {
        return isUpdateQuery;
    }

    public void setUpdateQuery(boolean updateQuery) {
        isUpdateQuery = updateQuery;
    }

    public int getPattern_count() {
        return pattern_count;
    }

    public void setPattern_count(int pattern_count) {
        this.pattern_count = pattern_count;
    }

    public String processQuery(String query) {
        if (this.pattern_count == -1) {
            return query;
        }
        Pattern pattern = Pattern.compile("\\[(.*?)\\]\\[pattern_count]");
        Matcher matcher = pattern.matcher(query);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
             String placeholderPattern = matcher.group(1);
            String repeatedPattern = String.join(",", Collections.nCopies(this.pattern_count, placeholderPattern));
            matcher.appendReplacement(result, repeatedPattern);
        }
        matcher.appendTail(result);
       return result.toString();
    }

    public <T> List<T> repeatList(List<T> inputList, int times) {
        if (times <= 0) {
            throw new IllegalArgumentException("Repeat count must be greater than 0.");
        }
        int size = inputList.size();
        List<T> result = new ArrayList<>(size * times); // Pre-size the list for better performance
        for (int i = 0; i < times; i++) {
            result.addAll(inputList);
        }
        return result;
    }
}
