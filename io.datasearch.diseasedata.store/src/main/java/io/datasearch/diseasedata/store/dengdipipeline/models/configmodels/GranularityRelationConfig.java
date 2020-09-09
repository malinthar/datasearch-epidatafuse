package io.datasearch.diseasedata.store.dengdipipeline.models.configmodels;

import java.util.HashMap;

/**
 * config class
 */
public class GranularityRelationConfig {

    private String featureTypeName;
    private String spatialGranularity;
    private String targetSpatialGranularity;
    private String targetTemporalGranularity;

    private String relationMappingMethod;
    private HashMap<String, String> customAttributes;

    public GranularityRelationConfig(String featureTypeName, String spatialGranularity, String relationMappingMethod,
                                     String targetSpatial, String targetTemporal) {
        this.customAttributes = new HashMap<String, String>();
        this.featureTypeName = featureTypeName;
        this.spatialGranularity = spatialGranularity;
        this.relationMappingMethod = relationMappingMethod;
        this.targetSpatialGranularity = targetSpatial;
        this.targetTemporalGranularity = targetTemporal;
    }

    public String getFeatureTypeName() {
        return this.featureTypeName;
    }

    public String getSpatialGranularity() {
        return this.spatialGranularity;
    }

    public String getRelationMappingMethod() {
        return this.relationMappingMethod;
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
