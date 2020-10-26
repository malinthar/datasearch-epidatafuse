package io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap;

/**
 * temporal map class
 */
public class TemporalGranularityMap {

    private String baseTemporalGranularity;
    private String targetTemporalGranularity;
    private String featureTypeName;
    private String relationMappingMethod;

    public TemporalGranularityMap(String baseTemporalGranularity, String targetTemporalGranularity,
                                  String featureTypeName, String relationMappingMethod) {
        this.baseTemporalGranularity = baseTemporalGranularity;
        this.targetTemporalGranularity = targetTemporalGranularity;
        this.featureTypeName = featureTypeName;
        this.relationMappingMethod = relationMappingMethod;
    }
}
