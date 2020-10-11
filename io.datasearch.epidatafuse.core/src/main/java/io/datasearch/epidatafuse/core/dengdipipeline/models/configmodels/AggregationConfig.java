package io.datasearch.epidatafuse.core.dengdipipeline.models.configmodels;

import java.util.HashMap;

/**
 * config class
 */
public class AggregationConfig {
    private String featureTypeName;
    private String indexCol;
    private String aggregationType;
    private String aggregationMethod;
    private String aggregationOn;
    private HashMap<String, String> customConfigs = new HashMap<String, String>();

    public AggregationConfig(String featureTypeName, String indexCol, String aggregationType, String aggregationMethod,
                             String aggregationOn, HashMap<String, String> customConfig) {
        this.featureTypeName = featureTypeName;
        this.indexCol = indexCol;
        this.aggregationType = aggregationType;
        this.aggregationMethod = aggregationMethod;
        this.aggregationOn = aggregationOn;
    }

    public String getFeatureTypeName() {
        return featureTypeName;
    }

    public String getIndexCol() {
        return indexCol;
    }

    public String getAggregationType() {
        return aggregationType;
    }

    public String getAggregationMethod() {
        return aggregationMethod;
    }

    public String getAggregationOn() {
        return aggregationOn;
    }

    public HashMap<String, String> getCustomConfigs() {
        return customConfigs;
    }

    public String getCustomConfig(String attrName) {
        if (this.customConfigs.containsKey(attrName)) {
            return this.customConfigs.get(attrName);
        } else {
            return null;
        }
    }
}
