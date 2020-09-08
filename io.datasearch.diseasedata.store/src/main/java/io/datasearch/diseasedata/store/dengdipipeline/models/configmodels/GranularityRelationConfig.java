package io.datasearch.diseasedata.store.dengdipipeline.models.configmodels;

import java.util.HashMap;

/**
 * config class
 */
public class GranularityRelationConfig {

    private String featureTypeName;
    private String spatialGranularity;
    private String relationMappingMethod;
    private HashMap<String, String> customAttributes;

    public GranularityRelationConfig(String featureTypeName, String spatialGranularity, String relationMappingMethod) {
        this.customAttributes = new HashMap<String, String>();
        this.featureTypeName = featureTypeName;
        this.spatialGranularity = spatialGranularity;
        this.relationMappingMethod = relationMappingMethod;
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

    public void setCustomAttributes(String attrName, String value) {
        this.customAttributes.put(attrName, value);
    }

    public String getCustomAttribute(String attrName) {
        return customAttributes.get(attrName);
    }
    /**/
}
