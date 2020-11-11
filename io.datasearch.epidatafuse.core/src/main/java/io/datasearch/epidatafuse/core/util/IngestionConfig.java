package io.datasearch.epidatafuse.core.util;

import java.util.List;
import java.util.Map;

/**
 * Single feature ingest.
 */
public class IngestionConfig {
    private static final String SIMPLE_FEATURE_TYPES_KEY = "simple_feature_types";
    private static final String GRANULARITY_FEATURE_TYPES_KEY = "granularity_feature_types";
    private static final String FEATURE_NAME_KEY = "feature_name";
    private static final String DATA_SOURCE_FILE_KEY = "data_source";
    private static final String DATA_SOURCE_TYPE_KEY = "type";
    private static final String DATA_SOURCE_FORMAT_KEY = "format";
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";
    private static final String TRANSFORMATIONS_KEY = "transformations";
    private static final String TRANSFORMATION_KEY = "transformation";

    private List<IngestConfig> featureConfigs;
    private List<Map<String, Object>> granularityConfigs;

    public IngestionConfig(Map<String, Object> configurations) {
        featureConfigs = (List<IngestConfig>) configurations.get(SIMPLE_FEATURE_TYPES_KEY);
        granularityConfigs = (List<Map<String, Object>>) configurations.get(GRANULARITY_FEATURE_TYPES_KEY);
    }

    public List<IngestConfig> getFeatureConfigs() {
        return featureConfigs;
    }

    public List<Map<String, Object>> getGranularityConfigs() {
        return granularityConfigs;
    }
}
