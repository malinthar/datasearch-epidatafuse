package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapping granules within a given radius
 */

public class DefaultMapper {

    public static final String MAPPER_NAME = "None(Default)";
    private static final Map<String, Object> ARGUMENTS = new HashMap<>();

    public static SpatialGranularityRelationMap buildDefaultMap(SimpleFeatureCollection targetGranuleSet,
                                                                SimpleFeatureCollection baseGranuleSet, String baseUUID,
                                                                String targetUUID) {
        SpatialGranularityRelationMap spatialMap = new SpatialGranularityRelationMap();
        SimpleFeatureIterator featureIt = targetGranuleSet.features();

        try {
            while (featureIt.hasNext()) {
                SimpleFeature next = featureIt.next();
                ArrayList<String> defaultGranule = new ArrayList<String>();
                defaultGranule.add(next.getAttribute(targetUUID).toString());
                spatialMap.addTargetToBasesMapping(next.getAttribute(targetUUID).toString(), defaultGranule);
            }
        } finally {
            featureIt.close();
        }
        return spatialMap;

    }

    public static Map<String, Object> getArguments() {
        return ARGUMENTS;
    }
}
