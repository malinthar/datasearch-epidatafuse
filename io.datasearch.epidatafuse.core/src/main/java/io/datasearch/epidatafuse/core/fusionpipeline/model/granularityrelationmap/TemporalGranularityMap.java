package io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod.TemporalRelationship;

/**
 * temporal map class
 */
public class TemporalGranularityMap {

    private String baseTemporalGranularity;
    private String targetTemporalGranularity;
    private String featureTypeName;
    private String relationMappingMethod;
    private long relationValue;

    public TemporalGranularityMap(String baseTemporalGranularity, String targetTemporalGranularity,
                                  String featureTypeName, String relationMappingMethod) {
        this.baseTemporalGranularity = baseTemporalGranularity;
        this.targetTemporalGranularity = targetTemporalGranularity;
        this.featureTypeName = featureTypeName;
        this.relationMappingMethod = relationMappingMethod;
        this.relationValue = TemporalRelationship.getRelationShip(baseTemporalGranularity, targetTemporalGranularity);
    }

    public String getBaseTemporalGranularity() {
        return baseTemporalGranularity;
    }

    public String getFeatureTypeName() {
        return featureTypeName;
    }

    public String getRelationMappingMethod() {
        return relationMappingMethod;
    }

    public String getTargetTemporalGranularity() {
        return targetTemporalGranularity;
    }

    public long getRelationValue() {
        return relationValue;
    }
}
