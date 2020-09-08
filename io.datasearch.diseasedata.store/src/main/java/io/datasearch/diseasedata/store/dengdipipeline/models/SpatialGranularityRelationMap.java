package io.datasearch.diseasedata.store.dengdipipeline.models;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * spatial map class
 */
public class SpatialGranularityRelationMap {

    private HashMap<String, ArrayList<String>> mappings;

    public SpatialGranularityRelationMap() {
        this.mappings = new HashMap<String, ArrayList<String>>();
    }

    public void addPoint(String targetGranuleId, ArrayList<String> baseGranuleIdSet) {
        this.mappings.put(targetGranuleId, baseGranuleIdSet);
    }

    public ArrayList<String> getBasePointIds(String targetGranuleId) {
        ArrayList<String> basePoints = this.mappings.get(targetGranuleId);
        return basePoints;
    }

    public HashMap<String, ArrayList<String>> getMap() {
        return this.mappings;
    }
}
