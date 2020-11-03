package io.datasearch.epidatafuse.core.dengdipipeline.models.configmodels;

import java.util.HashMap;

/**
 * config class
 */
public class AggregationConfig {
    private String featureTypeName;
    private String indexCol;
    private Boolean isASpatialInterpolation;
    private Boolean isATemporalInterpolation;
    private String spatialAggregationMethod;
    private String temporalAggregationMethod;
    private String aggregationOn;
    private HashMap<String, String> customConfigs = new HashMap<String, String>();

    public AggregationConfig(String featureTypeName, String indexCol, Boolean isASpatialInterpolation,
                             Boolean isATemporalInterpolation,
                             String spatialAggregationMethod, String temporalAggregationMethod,
                             String aggregationOn, HashMap<String, String> customConfig) {
        this.featureTypeName = featureTypeName;
        this.indexCol = indexCol;
        this.isASpatialInterpolation = isASpatialInterpolation;
        this.isATemporalInterpolation = isATemporalInterpolation;
        this.spatialAggregationMethod = spatialAggregationMethod;
        this.temporalAggregationMethod = temporalAggregationMethod;
        this.aggregationOn = aggregationOn;
    }

    public String getFeatureTypeName() {
        return featureTypeName;
    }

    public String getIndexCol() {
        return indexCol;
    }

    public Boolean isASpatialInterpolation() {
        return isASpatialInterpolation;
    }

    public Boolean isATemporalInterpolation() {
        return isATemporalInterpolation;
    }

    public String getSpatialAggregationMethod() {
        return spatialAggregationMethod;
    }

    public String getTemporalAggregationMethod() {
        return temporalAggregationMethod;
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
