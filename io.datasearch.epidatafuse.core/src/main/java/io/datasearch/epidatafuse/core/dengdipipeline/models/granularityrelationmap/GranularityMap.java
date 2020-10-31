package io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap;

/**
 * temporal map class
 */
public class GranularityMap {

    SpatialGranularityRelationMap spatialGranularityRelationMap;
    TemporalGranularityMap temporalGranularityMap;
    String featureTypeName;
    String baseSpatialGranularity;
    String baseTemporalGranularity;
    String targetSpatialGranularity;
    String targetTemporalGranularity;

    public GranularityMap(String featureTypeName, SpatialGranularityRelationMap spatialMap,
                          TemporalGranularityMap temporalMap, String baseSpatialGranularity,
                          String baseTemporalGranularity, String targetSpatialGranularity,
                          String targetTemporalGranularity
    ) {
        this.featureTypeName = featureTypeName;
        this.temporalGranularityMap = temporalMap;
        this.spatialGranularityRelationMap = spatialMap;
        this.baseSpatialGranularity = baseSpatialGranularity;
        this.baseTemporalGranularity = baseTemporalGranularity;
        this.targetSpatialGranularity = targetSpatialGranularity;
        this.targetTemporalGranularity = targetTemporalGranularity;
    }

    public String getFeatureTypeName() {
        return featureTypeName;
    }

    public TemporalGranularityMap getTemporalGranularityMap() {
        return temporalGranularityMap;
    }

    public SpatialGranularityRelationMap getSpatialGranularityRelationMap() {
        return spatialGranularityRelationMap;
    }

    public String getBaseSpatialGranularity() {
        return baseSpatialGranularity;
    }

    public String getBaseTemporalGranularity() {
        return baseTemporalGranularity;
    }

    public String getTargetSpatialGranularity() {
        return targetSpatialGranularity;
    }

    public String getTargetTemporalGranularity() {
        return targetTemporalGranularity;
    }
}
