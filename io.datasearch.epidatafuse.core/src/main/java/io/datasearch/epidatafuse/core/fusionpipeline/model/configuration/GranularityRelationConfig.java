package io.datasearch.epidatafuse.core.fusionpipeline.model.configuration;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod.MapperUtil;

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
    private static final String GRANULARITY_MAPPING_KEY = "granularity_mapping";
    private static final String MAPPING_METHOD_NAME_KEY = "method_name";
    private static final String MAPPING_ARGUMENTS_KEY = "mapping_arguments";
    private static final String MAPPING_ARGUMENT_NAME_KEY = "argument_name";
    private static final String MAPPING_ARGUMENT_VALUE_KEY = "argument_value";
    private static final String SPATIAL_MAPPING_METHOD_KEY = "spatial_mapping_method";
    private static final String TEMPORAL_MAPPING_METHOD_KEY = "temporal_mapping_method";
    private static final String TEMPORAL_MULTIPLIER_KEY = "temporal_multiplier";
    private static final String TARGET_TEMPORAL_MULTIPLIER_KEY = "target_temporal_multiplier";

    private String featureTypeName;
    private String spatialGranularity;
    private String temporalGranularity;
    private int temporalMultiplier;
    private String targetSpatialGranularity;
    private String targetTemporalGranularity;
    private int targetTemporalMultiplier;
    private String spatialRelationMappingMethod;
    private Map<String, Object> spatialMappingArguments;

    public GranularityRelationConfig(String featureTypeName, Map<String, Object> granularityConfig) {
        this.spatialMappingArguments = new HashMap<>();
        spatialMappingArguments = new HashMap<>();
        this.featureTypeName = featureTypeName;
        this.spatialGranularity = (String) granularityConfig.get(SPATIAL_GRANULARITY_KEY);
        this.temporalGranularity = (String) granularityConfig.get(TEMPORAL_GRANULARITY_KEY);
        this.targetSpatialGranularity = (String) granularityConfig.get(TARGET_SPATIAL_GRANULARITY_KEY);
        this.targetTemporalGranularity = (String) granularityConfig.get(TARGET_TEMPORAL_GRANULARITY_KEY);
        this.temporalMultiplier = (Integer) granularityConfig.get(TEMPORAL_MULTIPLIER_KEY);
        this.targetTemporalMultiplier = (Integer) granularityConfig.get(TARGET_TEMPORAL_MULTIPLIER_KEY);
        if (granularityConfig.get(GRANULARITY_MAPPING_KEY) != null) {
            Map<String, Object> mappingMethod = (Map<String, Object>) granularityConfig.get(GRANULARITY_MAPPING_KEY);
            if (mappingMethod != null) {
                Map<String, Object> spatialMap = (Map<String, Object>) mappingMethod.get(SPATIAL_MAPPING_METHOD_KEY);
                if (spatialMap != null) {
                    this.spatialRelationMappingMethod = (String) spatialMap.get(MAPPING_METHOD_NAME_KEY);
                    List<Map<String, String>> spatialArguments =
                            (List<Map<String, String>>) spatialMap.get(MAPPING_ARGUMENTS_KEY);
                    if (spatialArguments != null) {
                        for (Map<String, String> argument : spatialArguments) {
                            spatialMappingArguments.put(argument.get(MAPPING_ARGUMENT_NAME_KEY),
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
        return "default";
    }

    public String getTargetTemporalGranularity() {
        return this.targetTemporalGranularity;
    }

    public String getMappingAttribute(String attrName) {
        return (String) spatialMappingArguments.get(attrName);
    }

    public Map<String, Object> getSpatialMappingArguments() {
        if (spatialMappingArguments.size() == 0) {
            return MapperUtil.getMapper(this.spatialRelationMappingMethod);

        } else {
            return spatialMappingArguments;
        }

    }

    public int getTemporalMultiplier() {
        return temporalMultiplier;
    }

    public int getTargetTemporalMultiplier() {
        return targetTemporalMultiplier;
    }
}
