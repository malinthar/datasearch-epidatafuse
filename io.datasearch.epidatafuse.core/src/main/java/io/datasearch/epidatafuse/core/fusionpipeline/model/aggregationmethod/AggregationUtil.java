package io.datasearch.epidatafuse.core.fusionpipeline.model.aggregationmethod;

import java.util.HashMap;
import java.util.Map;

/**
 * Util class for Aggregation.
 */
public class AggregationUtil {
    private static final Map<String, Map<String, Object>> SPATIAL_AGGREGATORS = new HashMap<>();
    private static final Map<String, Map<String, Object>> TEMPORAL_AGGREGATORS = new HashMap<>();
    public static final String MEAN = "mean";
    public static final String SUM = "sum";
    public static final String MAX = "max";
    public static final String MIN = "min";
    public static final String INVERSE_DISTANCE = "InverseDistance";

    static {
        SPATIAL_AGGREGATORS.put(MEAN, new HashMap<>());
        SPATIAL_AGGREGATORS.put(SUM, new HashMap<>());
        SPATIAL_AGGREGATORS.put(MAX, new HashMap<>());
        SPATIAL_AGGREGATORS.put(MIN, new HashMap<>());
        SPATIAL_AGGREGATORS.put(INVERSE_DISTANCE, new HashMap<>());

        TEMPORAL_AGGREGATORS.put(MEAN, new HashMap<>());
        TEMPORAL_AGGREGATORS.put(SUM, new HashMap<>());
        TEMPORAL_AGGREGATORS.put(MAX, new HashMap<>());
        TEMPORAL_AGGREGATORS.put(MIN, new HashMap<>());
    }

    public static Map<String, Map<String, Object>> getSpatialAggregators() {
        return SPATIAL_AGGREGATORS;
    }

    public static Map<String, Map<String, Object>> getTemporalAggregators() {
        return TEMPORAL_AGGREGATORS;
    }

    public static Map<String, Object> getAggregator(String aggregatorName) {
        return SPATIAL_AGGREGATORS.get(aggregatorName);
    }
}
