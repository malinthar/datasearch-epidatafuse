package io.datasearch.epidatafuse.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * FeatureConfig when adding a new feature.
 */
public class FeatureConfig {
    private static final String PIPELINE_NAME_KEY = "pipeline_name";
    private static final String FEATURE_NAME_KEY = "feature_name";
    private static final String GRANULARITY_CONFIG_KEY = "granularity_config";
    private static final String ATTRIBUTES_KEY = "attributes";
    public static final String VARIABLE_TYPE_IDENTIFIER = "variable";
    public static final String GRANULARITY_TYPE_IDENTIFIER = "granularity";
    public static final String UUID_ATTRIBUTE_NAME_KEY = "uuid_attribute_name";
    public static final String AGGREGATION_CONFIG_KEY = "aggregation_config";
    private Map<String, Object> granularityRelationConfig;
    private List<Map<String, String>> aggregationConfig;
    private String pipelineName;
    private String featureName;
    private String featureType;
    private List<Map<String, String>> attributes;
    private String uuidAttributeName;
    private static final Logger logger = LoggerFactory.getLogger(FeatureConfig.class);

    public FeatureConfig(Map<String, Object> configurations, String type) {
        try {
            this.featureType = type;
            this.pipelineName = (String) configurations.get(PIPELINE_NAME_KEY);
            this.featureName = (String) configurations.get(FEATURE_NAME_KEY);
            this.attributes = (List<Map<String, String>>) configurations.get(ATTRIBUTES_KEY);
            if (VARIABLE_TYPE_IDENTIFIER.equals(type)) {
                this.granularityRelationConfig = (Map<String, Object>) configurations.get(GRANULARITY_CONFIG_KEY);
                this.aggregationConfig = (List<Map<String, String>>) configurations.get(AGGREGATION_CONFIG_KEY);
            } else if (GRANULARITY_TYPE_IDENTIFIER.equals(type)) {
                this.uuidAttributeName = (String) configurations.get(UUID_ATTRIBUTE_NAME_KEY);
            }
        } catch (Exception e) {
            throw e;
        }

    }

    public String getFeatureName() {
        return featureName;
    }

    public List<Map<String, String>> getAttributes() {
        return attributes;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public Map<String, Object> getGranularityRelationConfig() {
        return granularityRelationConfig;
    }

    public String getFeatureType() {
        return featureType;
    }

    public String getUuidAttributeName() {
        return uuidAttributeName;
    }

    public List<Map<String, String>> getAggregationConfig() {
        return this.aggregationConfig;
    }
}
