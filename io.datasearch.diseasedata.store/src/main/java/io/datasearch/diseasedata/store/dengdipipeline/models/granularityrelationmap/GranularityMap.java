package io.datasearch.diseasedata.store.dengdipipeline.models.granularityrelationmap;

/**
 * temporal map class
 */
public class GranularityMap {

    SpatialGranularityRelationMap spatialGranularityRelationMap;
    TemporalGranularityMap temporalGranularityMap;
    String featureTypeName;

    public GranularityMap(String featureTypeName, SpatialGranularityRelationMap spatialMap,
                          TemporalGranularityMap temporalMap
    ) {
        this.featureTypeName = featureTypeName;
        this.temporalGranularityMap = temporalMap;
        this.spatialGranularityRelationMap = spatialMap;
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
}
