package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * containing objects mapping method
 */

public class TemporalRelationship {
    private static final Logger logger = LoggerFactory.getLogger(TemporalRelationship.class);
    private static final String MINUTE = "minute";
    private static final String HOUR = "hour";
    private static final String DAY = "day";
    private static final String WEEK = "week";
    private static final String MONTH = "month";
    private static final String YEAR = "year";

    private static final List<String> TEMPORAL_UNITS_LIST;
    private static Table<String, String, Integer> relationshipTable = HashBasedTable.create();
    private static final Map<String, Long> GRANULARITY_MAP;
    private static final HashMap<String, Integer> GRANULARITY_MAP_HOURS;

    private static final Table<String, String, Integer> INTERPOLATION_DIVIDE_FACTORS = HashBasedTable.create();

    static {
        relationshipTable.put("day", "hour", 24);
        relationshipTable.put("week", "day", 168);
        relationshipTable.put("month", "day", 720);
        relationshipTable.put("year", "day", 8760);
        relationshipTable.put("hour", "hour", 1);
        relationshipTable.put("hour", "week", 168);
        relationshipTable.put("day", "day", 24);
        relationshipTable.put("week", "week", 168);
        relationshipTable.put("year", "year", 8760);

        TEMPORAL_UNITS_LIST = new ArrayList<>();
        TEMPORAL_UNITS_LIST.addAll(Arrays.asList(MINUTE, HOUR, DAY, WEEK, MONTH, YEAR));

        GRANULARITY_MAP = new HashMap<>();
        GRANULARITY_MAP.put(MINUTE, 1000 * 60L);
        GRANULARITY_MAP.put(HOUR, 1000 * 60 * 60L);
        GRANULARITY_MAP.put(DAY, 1000 * 60 * 60 * 2L);
        GRANULARITY_MAP.put(WEEK, 1000 * 60 * 60 * 24 * 7L);
        GRANULARITY_MAP.put(MONTH, 1000 * 60 * 60 * 24 * 30L);
        GRANULARITY_MAP.put(YEAR, 1000 * 60 * 60 * 24 * 365L);

        GRANULARITY_MAP_HOURS = new HashMap<String, Integer>();
        GRANULARITY_MAP_HOURS.put(HOUR, 1);
        GRANULARITY_MAP_HOURS.put(DAY, 24);
        GRANULARITY_MAP_HOURS.put(WEEK, 168);
        GRANULARITY_MAP_HOURS.put(MONTH, 720);
        GRANULARITY_MAP_HOURS.put(YEAR, 8760);

        INTERPOLATION_DIVIDE_FACTORS.put("year", "year", 1);
        INTERPOLATION_DIVIDE_FACTORS.put("year", "month", 12);
        INTERPOLATION_DIVIDE_FACTORS.put("year", "week", 52);
        INTERPOLATION_DIVIDE_FACTORS.put("year", "day", 365);
        INTERPOLATION_DIVIDE_FACTORS.put("year", "hour", 8760);
        INTERPOLATION_DIVIDE_FACTORS.put("year", "minute", 525600);

        INTERPOLATION_DIVIDE_FACTORS.put("month", "month", 1);
        INTERPOLATION_DIVIDE_FACTORS.put("month", "week", 4);
        INTERPOLATION_DIVIDE_FACTORS.put("month", "day", 30);
        INTERPOLATION_DIVIDE_FACTORS.put("month", "hour", 720);
        INTERPOLATION_DIVIDE_FACTORS.put("month", "minute", 43200);

        INTERPOLATION_DIVIDE_FACTORS.put("week", "week", 1);
        INTERPOLATION_DIVIDE_FACTORS.put("week", "day", 7);
        INTERPOLATION_DIVIDE_FACTORS.put("week", "hour", 168);
        INTERPOLATION_DIVIDE_FACTORS.put("week", "minute", 10080);

        INTERPOLATION_DIVIDE_FACTORS.put("day", "day", 1);
        INTERPOLATION_DIVIDE_FACTORS.put("day", "hour", 24);
        INTERPOLATION_DIVIDE_FACTORS.put("day", "minute", 1440);

        INTERPOLATION_DIVIDE_FACTORS.put("hour", "hour", 1);
        INTERPOLATION_DIVIDE_FACTORS.put("hour", "minute", 60);

    }

//    public static long getRelationShip(String row, String col) {
//        long value;
//        if (relationshipTable.contains(row, col)) {
//            value = relationshipTable.get(row, col);
//        } else if (relationshipTable.contains(col, row)) {
//            value = relationshipTable.get(col, row);
//        } else {
//            value = 1;
//        }
//        return value;
//    }

    public static long getRelationShip(String baseTemporalGranularity, int baseMultiplier,
                                       String targetTemporalGranularity, int targetMultiplier) {
        long value = 1;
        if (GRANULARITY_MAP_HOURS.containsKey(baseTemporalGranularity) &&
                GRANULARITY_MAP_HOURS.containsKey(targetTemporalGranularity)) {
            if (TEMPORAL_UNITS_LIST.indexOf(baseTemporalGranularity) >
                    TEMPORAL_UNITS_LIST.indexOf(targetTemporalGranularity)) {
                value = GRANULARITY_MAP_HOURS.get(baseTemporalGranularity) * baseMultiplier;
            } else {
                value = GRANULARITY_MAP_HOURS.get(targetTemporalGranularity) * targetMultiplier;
            }
        }
        return value;
    }

    public static long getGranularityToSeconds(String granularity) {
        return GRANULARITY_MAP.get(granularity);
    }

    public static List<String> getTemporalUnitsList() {
        return TEMPORAL_UNITS_LIST;
    }

    public static long getInterpolationDivideFactor(String baseTemporalGranularity, int baseMultiplier,
                                                    String targetTemporalGranularity, int targetMultiplier) {
        long value;
        if (TEMPORAL_UNITS_LIST.contains(baseTemporalGranularity) &&
                TEMPORAL_UNITS_LIST.contains(targetTemporalGranularity)) {
            if (TEMPORAL_UNITS_LIST.indexOf(baseTemporalGranularity) >
                    TEMPORAL_UNITS_LIST.indexOf(targetTemporalGranularity)) {
                if (INTERPOLATION_DIVIDE_FACTORS.contains(baseTemporalGranularity, targetTemporalGranularity)) {
                    value = INTERPOLATION_DIVIDE_FACTORS.get(baseTemporalGranularity, targetTemporalGranularity) *
                            (baseMultiplier / targetMultiplier);
                } else {
                    value = 0;
                }
            } else {
                value = 0;
            }
        } else {
            value = 0;
        }
        return value;
    }
}
