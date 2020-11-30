package io.datasearch.epidatafuse.core.fusionpipeline.model.configuration;

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
    private String featureTypeName;
    private String indexCol;
    private Boolean isASpatialInterpolation;
    private Boolean isATemporalInterpolation;
    private String spatialAggregationMethod;
    private String temporalAggregationMethod;
    private String aggregationOn;
    private List<String> aggregatedAttributes;
    private Map<String, String> spatialArguments;
    private Map<String, String> temporalArguments;

    public AggregationConfig(String featureTypeName, Map<String, Object> aggregationConfig) {

        this.featureTypeName = featureTypeName;
        this.indexCol = "stationID";
        this.isASpatialInterpolation = false;
        this.isATemporalInterpolation = false;
        this.spatialArguments = new HashMap<>();
        this.temporalArguments = new HashMap<>();
        if (aggregationConfig != null) {
            this.aggregatedAttributes = (List<String>) aggregationConfig.get(AGGREGATED_ATTRIBUTES_KEY);
            this.aggregationOn = aggregatedAttributes.get(0); //todo:remove after making fuse engine compatible;
            Map<String, Object> spatialAggregation =
                    (Map<String, Object>) aggregationConfig.get(SPATIAL_AGGREGATION_KEY);
            Map<String, Object> temporalAggregation =
                    (Map<String, Object>) aggregationConfig.get(SPATIAL_AGGREGATION_KEY);
            if (spatialAggregation != null) {
                this.spatialAggregationMethod = (String) spatialAggregation.get(METHOD_NAME_KEY);
                List<Map<String, String>> args =
                        (List<Map<String, String>>) spatialAggregation.get(AGGREGATION_ARGUMENTS_KEY);
                if (args != null) {
                    for (Map<String, String> argument : args) {
                        spatialArguments.put(argument.get(ARGUMENT_NAME_KEY),
                                argument.get(ARGUMENT_VALUE_KEY));
                    }
                }
            }
            if (temporalAggregation != null) {
                this.temporalAggregationMethod = (String) temporalAggregation.get(METHOD_NAME_KEY);
                List<Map<String, String>> args =
                        (List<Map<String, String>>) temporalAggregation.get(AGGREGATION_ARGUMENTS_KEY);
                if (args != null) {
                    for (Map<String, String> argument : args) {
                        temporalArguments.put(argument.get(ARGUMENT_NAME_KEY),
                                argument.get(ARGUMENT_VALUE_KEY));
                    }
                }
            }
        }
    }

    public String getFeatureTypeName() {
        return featureTypeName;
    }

    public String getIndexCol() {
        return indexCol;
    }

    public Boolean isASpatialInterpolation() {
        return isASpatialInterpolation;
    }

    public Boolean isATemporalInterpolation() {
        return isATemporalInterpolation;
    }

    public String getSpatialAggregationMethod() {
        return spatialAggregationMethod;
    }

    public String getTemporalAggregationMethod() {
        return temporalAggregationMethod;
    }

    public String getAggregationOn() {
        return aggregationOn;
    }

    public Map<String, String> getSpatialArguments() {
        return spatialArguments;
    }

    public Map<String, String> getTemporalArguments() {
        return temporalArguments;
    }

    public String getSpatialArgumentValue(String arg) {
        if (this.spatialArguments.containsKey(arg)) {
            return this.spatialArguments.get(arg);
        } else {
            return null;
        }
    }

    public String getTemporalArgumentValue(String arg) {
        if (this.temporalArguments.containsKey(arg)) {
            return this.temporalArguments.get(arg);
        } else {
            return null;
        }
    }

}
