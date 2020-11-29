package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * containing objects mapping method
 */

public class TemporalRelationship {
    private static final Logger logger = LoggerFactory.getLogger(TemporalRelationship.class);

    private static Table<String, String, Integer> relationshipTable = HashBasedTable.create();

    static {
        relationshipTable.put("day", "hour", 24);
        relationshipTable.put("week", "day", 168);
        relationshipTable.put("month", "day", 720);
        relationshipTable.put("year", "day", 8760);
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
}
