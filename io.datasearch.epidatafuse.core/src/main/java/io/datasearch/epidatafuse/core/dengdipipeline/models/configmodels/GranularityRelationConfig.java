package io.datasearch.epidatafuse.core.dengdipipeline.models.configmodels;

import java.util.HashMap;

/**
 * config class
 */
public class GranularityRelationConfig {

    private String featureTypeName;
    private String spatialGranularity;
    private String temporalGranularity;
    private String targetSpatialGranularity;
    private String targetTemporalGranularity;

    private String spatialRelationMappingMethod;
    private String temporalRelationMappingMethod;
    private HashMap<String, String> customAttributes;

    public GranularityRelationConfig(String featureTypeName, String spatialGranularity, String temporalGranularity,
                                     String spatialRelationMappingMethod, String temporalRelationMappingMethod,
                                     String targetSpatial, String targetTemporal) {
        this.customAttributes = new HashMap<String, String>();
        this.featureTypeName = featureTypeName;
        this.spatialGranularity = spatialGranularity;
        this.temporalGranularity = temporalGranularity;
        this.spatialRelationMappingMethod = spatialRelationMappingMethod;
        this.temporalRelationMappingMethod = temporalRelationMappingMethod;
        this.targetSpatialGranularity = targetSpatial;
        this.targetTemporalGranularity = targetTemporal;
    }

    public String getFeatureTypeName() {
        return this.featureTypeName;
    }

    public String getSpatialGranularity() {
        return this.spatialGranularity;
    }

    public String getTemporalGranularity() {
        return this.temporalGranularity;
    }

    public String getSpatialRelationMappingMethod() {
        return this.spatialRelationMappingMethod;
    }

    public String getTemporalRelationMappingMethod() {
        return this.temporalRelationMappingMethod;
    }

    public String getTargetSpatialGranularity() {
        return this.targetSpatialGranularity;
    }

    public String getTargetTemporalGranularity() {
        return this.targetTemporalGranularity;
    }

    public void setCustomAttributes(String attrName, String value) {
        this.customAttributes.put(attrName, value);
    }

    public String getCustomAttribute(String attrName) {
        return customAttributes.get(attrName);
    }
    /**/
}
