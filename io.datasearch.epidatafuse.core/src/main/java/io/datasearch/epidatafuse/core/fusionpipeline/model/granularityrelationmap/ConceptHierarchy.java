package io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap;

import java.util.HashMap;
import java.util.Map;

/**
 * User configuration of coarser than finer than relationship.
 */
public class ConceptHierarchy {
    private Map<Integer, String> conceptHierarchy;

    public ConceptHierarchy() {
        this.conceptHierarchy = new HashMap<>();
    }

    public void addNewConcept(String granularity, Integer level) {
        conceptHierarchy.put(level, granularity);
    }

    public int findLevel(String granularity) {
        for (Map.Entry entry : conceptHierarchy.entrySet()) {
            if (entry.getValue().equals(granularity)) {
                return (int) entry.getKey();
            }
        }
        return 0;
    }

    public boolean isCoarserThan(String granularity1, String granularity2) {
        int level1 = findLevel(granularity1);
        int level2 = findLevel(granularity2);
        return level1 > level2;
    }

    public boolean isFinerThan(String granularity1, String granularity2) {
        int level1 = findLevel(granularity1);
        int level2 = findLevel(granularity2);
        return level1 < level2;
    }
}
