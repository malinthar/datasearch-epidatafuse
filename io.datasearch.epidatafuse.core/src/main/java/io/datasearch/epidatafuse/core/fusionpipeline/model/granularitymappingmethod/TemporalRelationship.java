package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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
        TEMPORAL_UNITS_LIST.addAll(Arrays.asList(HOUR, DAY, WEEK, MONTH, YEAR));
    }

    public static long getRelationShip(String row, String col) {
        long value;
        if (relationshipTable.contains(row, col)) {
            value = relationshipTable.get(row, col);
        } else if (relationshipTable.contains(col, row)) {
            value = relationshipTable.get(col, row);
        } else {
            value = 0;
        }
        return value;
    }

    public static List<String> getTemporalUnitsList() {
        return TEMPORAL_UNITS_LIST;
    }
}
