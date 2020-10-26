package io.datasearch.epidatafuse.core.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Config file for initializing pipeline.
 */
public class SchemaConfig {
    private static final String SIMPLE_FEATURE_TYPES_KEY = "simple_feature_types";
    private static final String SPATIAL_GRANULARITIES_KEY = "spatial_granularities";
    private static final String TEMPORAL_GRANULARITIES_KEY = "temporal_granularities";
    private static final String PIPELINE_NAME_KEY = "temporal_granularities";
    private static final String FEATURE_NAME_KEY = "feature_name";
    private static final String GRANULARITY_RELATION_CONFIG_KEY = "granularity_config";
    private static final String VARIABLE_TYPE_IDENTIFIER = "variable";
    private static final String GRANULARITY_TYPE_IDENTIFIER = "granularity";

    private List<FeatureConfig> simpleFeatureSchemaConfigs;
    private List<FeatureConfig> spatialGranularitySchemaConfigs;
    private List<FeatureConfig> temporalGranularitySchemaConfigs;
    private Map<String, Map<String, Object>> granularityRelationConfigs;
    private String pipelineName;

    public SchemaConfig(Map<String, Object> configurations) {
        this.pipelineName = (String) configurations.get(PIPELINE_NAME_KEY);
        this.simpleFeatureSchemaConfigs = new ArrayList<>();
        this.spatialGranularitySchemaConfigs = new ArrayList<>();
        this.temporalGranularitySchemaConfigs = new ArrayList<>();
        this.granularityRelationConfigs = new HashMap<>();
        List<Map<String, Object>> featureConfigs =
                (List<Map<String, Object>>) configurations.get(SIMPLE_FEATURE_TYPES_KEY);
        for (Map<String, Object> featureConfig : featureConfigs) {
            simpleFeatureSchemaConfigs.add(new FeatureConfig(featureConfig, VARIABLE_TYPE_IDENTIFIER));
        }
        List<Map<String, Object>> spatialGranularityConfigs =
                (List<Map<String, Object>>) configurations.get(SPATIAL_GRANULARITIES_KEY);
        for (Map<String, Object> spatialGranularityConfig : spatialGranularityConfigs) {
            spatialGranularitySchemaConfigs.add(new FeatureConfig(spatialGranularityConfig,
                    GRANULARITY_TYPE_IDENTIFIER));
        }
        List<Map<String, Object>> temporalGranularityConfigs =
                (List<Map<String, Object>>) configurations.get(TEMPORAL_GRANULARITIES_KEY);
        for (Map<String, Object> temporalGranularityConfig : temporalGranularityConfigs) {
            spatialGranularitySchemaConfigs.add(new FeatureConfig(temporalGranularityConfig,
                    GRANULARITY_TYPE_IDENTIFIER));
        }
        simpleFeatureSchemaConfigs.forEach(config -> {
            granularityRelationConfigs.put(config.getFeatureName(), config.getGranularityRelationConfig());
        });
    }

    public List<FeatureConfig> getSimpleFeatureSchemaConfigs() {
        return simpleFeatureSchemaConfigs;
    }

    public List<FeatureConfig> getSpatialGranularitySchemaConfigs() {
        return spatialGranularitySchemaConfigs;
    }

    public List<FeatureConfig> getTemporalGranularitySchemaConfigs() {
        return temporalGranularitySchemaConfigs;
    }

    public Map<String, Map<String, Object>> getGranularityRelationConfigs() {
        return this.granularityRelationConfigs;
    }

    public String getPipelineName() {
        return pipelineName;
    }
}
