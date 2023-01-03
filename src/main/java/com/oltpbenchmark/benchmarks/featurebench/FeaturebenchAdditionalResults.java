package com.oltpbenchmark.benchmarks.featurebench;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class FeaturebenchAdditionalResults {

    public Map<String, JSONObject> explainAnalyze = null;

    public JSONObject pgStats = null;

    public FeaturebenchAdditionalResults() {
        this.explainAnalyze = new HashMap<>();
        this.pgStats = new JSONObject();
    }

    public Map<String, JSONObject> getExplainAnalyze() {
        return explainAnalyze;
    }

    public void setExplainAnalyze(Map<String, JSONObject> explainAnalyze) {
        this.explainAnalyze = explainAnalyze;
    }

    public JSONObject getPgStats() {
        return pgStats;
    }

    public void setPgStats(JSONObject pgStats) {
        this.pgStats = pgStats;
    }

}
