package io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod.TemporalRelationship;

/**
 * temporal map class
 */
public class TemporalGranularityMap {

    private String baseTemporalGranularity;
    private int baseTemporalMultiplier;
    private String targetTemporalGranularity;
    private int targetTemporalMultiplier;
    private String featureTypeName;
    private String relationMappingMethod;
    private long relationValue;
    private long interpolationDivideFactor;

    public TemporalGranularityMap(String baseTemporalGranularity, int baseTemporalMultiplier,
                                  String targetTemporalGranularity, int targetTemporalMultiplier,
                                  String featureTypeName, String relationMappingMethod) {
        this.baseTemporalGranularity = baseTemporalGranularity;
        this.baseTemporalMultiplier = baseTemporalMultiplier;
        this.targetTemporalGranularity = targetTemporalGranularity;
        this.targetTemporalMultiplier = targetTemporalMultiplier;
        this.featureTypeName = featureTypeName;
        this.relationMappingMethod = relationMappingMethod;
        this.relationValue = TemporalRelationship
                .getRelationShip(baseTemporalGranularity, baseTemporalMultiplier, targetTemporalGranularity,
                        targetTemporalMultiplier);
        this.interpolationDivideFactor =
                TemporalRelationship.getInterpolationDivideFactor(baseTemporalGranularity, baseTemporalMultiplier,
                        targetTemporalGranularity, targetTemporalMultiplier);
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

    public long getInterpolationDivideFactor() {
        return interpolationDivideFactor;
    }
}
