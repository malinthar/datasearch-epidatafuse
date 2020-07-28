package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * nearest
 */
public class NearestPointGranularityMap extends GranularityMap {
    private String featureType;
    private Map<String, ArrayList<String>> mappings;

    public NearestPointGranularityMap(String featureType) {
        this.featureType = featureType;
        this.mappings = new HashMap<String, ArrayList<String>>();
    }

    public void addPoint(String basePoint, ArrayList<String> nearestPoints) {
        this.mappings.put(basePoint, nearestPoints);
    }

    public ArrayList<String> getNearestPoints(String basePoint) {
        ArrayList<String> nearestPoints = this.mappings.get(basePoint);
        return nearestPoints;
    }

    public Map<String, ArrayList<String>> getMap() {
        return this.mappings;
    }

}
