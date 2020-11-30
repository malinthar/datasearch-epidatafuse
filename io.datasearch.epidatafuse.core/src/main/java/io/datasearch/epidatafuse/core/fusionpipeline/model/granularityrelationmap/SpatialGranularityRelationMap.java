package io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * spatial map class
 */
public class SpatialGranularityRelationMap {

    private Map<String, ArrayList<String>> targetToBasesMap;

    public SpatialGranularityRelationMap() {
        this.targetToBasesMap = new HashMap<>();
    }

    public void addTargetToBasesMapping(String targetGranuleId, ArrayList<String> baseGranuleIdSet) {
        this.targetToBasesMap.put(targetGranuleId, baseGranuleIdSet);
    }

    public ArrayList<String> getBaseGranuleIds(String targetGranuleId) {
        ArrayList<String> baseGranules = this.targetToBasesMap.get(targetGranuleId);
        return baseGranules;
    }

    public Map<String, ArrayList<String>> getMap() {
        return this.targetToBasesMap;
    }
}
