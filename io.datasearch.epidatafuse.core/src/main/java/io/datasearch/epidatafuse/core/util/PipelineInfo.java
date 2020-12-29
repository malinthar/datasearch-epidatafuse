package io.datasearch.epidatafuse.core.util;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.AggregationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.GranularityRelationConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pipeline information.
 */
public class PipelineInfo {
    private String pipelineName;
    private Map<String, List<Map<String, String>>> features;
    private Map<String, List<Map<String, String>>> granularities;
    private Map<String, GranularityRelationConfig> granularityConfigs;
    private Map<String, AggregationConfig> aggregationConfigs;
    private Map<String, String> granules = new HashMap<>();
    private static final String MOH_KEY = "moh";
    private static final String WEATHER_STATION_KEY = "weatherstation";
    private static final String MOH_PATH = "./moh/shapefile/SL_MOH";
    private static final String WEATHER_STATION_PATH = "./shapefile/sl_weatherstation";

    public PipelineInfo(String pipelineName, Map<String, SimpleFeatureTypeSchema> features,
                        Map<String, SimpleFeatureTypeSchema> granularities,
                        Map<String, GranularityRelationConfig> granularityConfigs,
                        Map<String, AggregationConfig> aggregationConfigs) {
        this.pipelineName = pipelineName;
        this.features = new HashMap<>();
        this.granularities = new HashMap<>();
        this.granularityConfigs = granularityConfigs;
        this.aggregationConfigs = aggregationConfigs;
        features.entrySet().forEach(entry -> {
            this.features.put(entry.getKey(), entry.getValue().getAttributes());
        });
        granularities.entrySet().forEach(entry -> {
            this.granularities.put(entry.getKey(), entry.getValue().getAttributes());
            if (MOH_KEY.equals(entry.getKey())) {
                granules.put(entry.getKey(), "SL_MOH.zip");
            } else {
                granules.put(entry.getKey(), "SL_MOH.zip");
            }
        });

    }

    public String getPipelineName() {
        return pipelineName;
    }

    public Map<String, List<Map<String, String>>> getFeatures() {
        return features;
    }

    public Map<String, List<Map<String, String>>> getGranularities() {
        return granularities;
    }

    public Map<String, AggregationConfig> getAggregationConfigs() {
        return aggregationConfigs;
    }

    public Map<String, GranularityRelationConfig> getGranularityConfigs() {
        return granularityConfigs;
    }

    public Map<String, String> getGranules() {
        return granules;
    }
}
