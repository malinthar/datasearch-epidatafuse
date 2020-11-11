package io.datasearch.epidatafuse.core.fusionpipeline.model.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Granularity relations.
 */
public class GranularityRelationConfig {

    private static final String SPATIAL_GRANULARITY_KEY = "spatial_granularity";
    private static final String TEMPORAL_GRANULARITY_KEY = "temporal_granularity";
    private static final String TARGET_SPATIAL_GRANULARITY_KEY = "target_spatial_granularity";
    private static final String TARGET_TEMPORAL_GRANULARITY_KEY = "target_temporal_granularity";
    private static final String MAPPING_METHOD_KEY = "mapping_method";
    private static final String MAPPING_METHOD_NAME_KEY = "method_name";
    private static final String MAPPING_ARGUMENTS_KEY = "mapping_arguments";
    private static final String MAPPING_ARGUMENT_NAME_KEY = "argument_name";
    private static final String MAPPING_ARGUMENT_VALUE_KEY = "argument_value";
    private static final String SPATIAL_MAPPING_METHOD_KEY = "spatial_mapping_method";
    private static final String TEMPORAL_MAPPING_METHOD_KEY = "temporal_mapping_method";

    private String featureTypeName;
    private String spatialGranularity;
    private String temporalGranularity;
    private String targetSpatialGranularity;
    private String targetTemporalGranularity;
    private String spatialRelationMappingMethod;
    private String temporalRelationMappingMethod;
    private Map<String, String> customSpatialMappingAttributes;
    private Map<String, String> customTemporalMappingAttributes;

    public GranularityRelationConfig(String featureTypeName, Map<String, Object> granularityConfig) {
        this.customSpatialMappingAttributes = new HashMap<>();
        customSpatialMappingAttributes = new HashMap<>();
        customTemporalMappingAttributes = new HashMap<>();
        this.featureTypeName = featureTypeName;
        this.spatialGranularity = (String) granularityConfig.get(SPATIAL_GRANULARITY_KEY);
        this.temporalGranularity = (String) granularityConfig.get(TEMPORAL_GRANULARITY_KEY);
        this.targetSpatialGranularity = (String) granularityConfig.get(TARGET_SPATIAL_GRANULARITY_KEY);
        this.targetTemporalGranularity = (String) granularityConfig.get(TARGET_TEMPORAL_GRANULARITY_KEY);
        if (granularityConfig.get(MAPPING_METHOD_KEY) != null) {
            Map<String, Object> mappingMethod = (Map<String, Object>) granularityConfig.get(MAPPING_METHOD_KEY);
            if (mappingMethod != null) {
                Map<String, Object> spatialMap = (Map<String, Object>) mappingMethod.get(SPATIAL_MAPPING_METHOD_KEY);
                Map<String, Object> temporalMap = (Map<String, Object>) mappingMethod.get(TEMPORAL_MAPPING_METHOD_KEY);
                if (spatialMap != null) {
                    this.spatialRelationMappingMethod = (String) spatialMap.get(MAPPING_METHOD_NAME_KEY);
                    List<Map<String, String>> arguments =
                            (List<Map<String, String>>) spatialMap.get(MAPPING_ARGUMENTS_KEY);
                    if (arguments != null) {
                        for (Map<String, String> argument : arguments) {
                            customSpatialMappingAttributes.put(argument.get(MAPPING_ARGUMENT_NAME_KEY),
                                    argument.get(MAPPING_ARGUMENT_VALUE_KEY));
                        }
                    }
                }
                if (temporalMap != null) {
                    this.temporalRelationMappingMethod = (String) temporalMap.get(MAPPING_METHOD_NAME_KEY);
                    List<Map<String, String>> arguments =
                            (List<Map<String, String>>) temporalMap.get(MAPPING_ARGUMENTS_KEY);
                    if (arguments != null) {
                        for (Map<String, String> argument : arguments) {
                            customTemporalMappingAttributes.put(argument.get(MAPPING_ARGUMENT_NAME_KEY),
                                    argument.get(MAPPING_ARGUMENT_VALUE_KEY));
                        }
                    }
                }
            }
        }

    }

    public String getFeatureTypeName() {
        return this.featureTypeName;
    }

    public String getSpatialGranularity() {
        return this.spatialGranularity;
    }

    public String getTemporalGranularity() {
        return this.temporalGranularity;
    }

    public String getTargetSpatialGranularity() {
        return this.targetSpatialGranularity;
    }

    public String getSpatialRelationMappingMethod() {
        return this.spatialRelationMappingMethod;
    }

    public String getTemporalRelationMappingMethod() {
        return this.temporalRelationMappingMethod;
    }

    public String getTargetTemporalGranularity() {
        return this.targetTemporalGranularity;
    }

    public void setMappingAttribute(String attrName, String value) {
        this.customSpatialMappingAttributes.put(attrName, value);
    }

    public String getMappingAttribute(String attrName) {
        return customSpatialMappingAttributes.get(attrName);
    }
}
