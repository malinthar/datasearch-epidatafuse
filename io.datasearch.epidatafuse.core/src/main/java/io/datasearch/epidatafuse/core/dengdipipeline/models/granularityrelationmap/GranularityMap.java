package io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap;

/**
 * temporal map class
 */
public class GranularityMap {

    SpatialGranularityRelationMap spatialGranularityRelationMap;
    TemporalGranularityMap temporalGranularityMap;
    String featureTypeName;
    String targetSpatialGranularity;
    String targetTemporalGranularity;

    public GranularityMap(String featureTypeName, SpatialGranularityRelationMap spatialMap,
                          TemporalGranularityMap temporalMap, String targetSpatialGranularity,
                          String targetTemporalGranularity
    ) {
        this.featureTypeName = featureTypeName;
        this.temporalGranularityMap = temporalMap;
        this.spatialGranularityRelationMap = spatialMap;
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

    public String getTargetSpatialGranularity() {
        return targetSpatialGranularity;
    }

    public String getTargetTemporalGranularity() {
        return targetTemporalGranularity;
    }
}
