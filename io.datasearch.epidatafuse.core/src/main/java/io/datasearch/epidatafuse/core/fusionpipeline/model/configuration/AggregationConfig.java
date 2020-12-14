package io.datasearch.epidatafuse.core.fusionpipeline.model.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * config class
 */
public class AggregationConfig {

    private static final String SPATIAL_AGGREGATION_KEY = "spatial_aggregation";
    private static final String TEMPORAL_AGGREGATION_KEY = "temporal_aggregation";
    private static final String METHOD_NAME_KEY = "method_name";
    private static final String AGGREGATION_ARGUMENTS_KEY = "aggregation_arguments";
    private static final String ARGUMENT_NAME_KEY = "argument_name";
    private static final String ARGUMENT_VALUE_KEY = "argument_value";
    private static final String AGGREGATED_ATTRIBUTES_KEY = "aggregated_attributes";
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";

    private String featureTypeName;

    private Boolean isASpatialInterpolation;
    private Boolean isATemporalInterpolation;
    private String aggregationOn;
    private List<String> aggregatedAttributes;
    private Map<String, Map<String, String>> aggregationMap;

    public AggregationConfig(String featureTypeName, List<Map<String, String>> aggregationConfig) {

        this.featureTypeName = featureTypeName;
        this.isASpatialInterpolation = false;
        this.isATemporalInterpolation = false;
        this.aggregationMap = new HashMap<>();
        this.aggregatedAttributes = new ArrayList<>();
        if (aggregationConfig != null) {
            aggregationConfig.forEach(record -> {
                aggregatedAttributes.add(record.get(ATTRIBUTE_NAME_KEY));
                aggregationMap.put(record.get(ATTRIBUTE_NAME_KEY), record);
            });
            this.aggregationOn = aggregatedAttributes.get(0); //todo:remove after making fuse engine compatible
        }

    }

    public String getFeatureTypeName() {
        return featureTypeName;
    }

    public Boolean isASpatialInterpolation() {
        return isASpatialInterpolation;
    }

    public Boolean isATemporalInterpolation() {
        return isATemporalInterpolation;
    }

    public String getSpatialAggregationMethod(String attributeName) {
        return aggregationMap.get(attributeName).get(SPATIAL_AGGREGATION_KEY);
    }

    public String getTemporalAggregationMethod(String attributeName) {
        return aggregationMap.get(attributeName).get(TEMPORAL_AGGREGATION_KEY);
    }

    public String getAggregationOn() {
        return aggregationOn;
    }
}
