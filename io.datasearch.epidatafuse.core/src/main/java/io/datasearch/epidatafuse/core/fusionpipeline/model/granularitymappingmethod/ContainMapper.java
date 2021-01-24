package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * containing objects mapping method
 */
public class ContainMapper {
    public static final String MAPPER_NAME = "Contains";
    private static final Map<String, Object> ARGUMENTS = new HashMap<>();

    public static SpatialGranularityRelationMap buildContainMap(SimpleFeatureCollection targetGranuleSet,
                                                                SimpleFeatureCollection baseGranuleSet, String baseUUID,
                                                                String targetUUID) {
        SpatialGranularityRelationMap spatialMap = new SpatialGranularityRelationMap();
        SimpleFeatureIterator featureIt = targetGranuleSet.features();
        try {
            while (featureIt.hasNext()) {
                SimpleFeature next = featureIt.next();
                ArrayList<String> containsMap = contains(next, baseGranuleSet, baseUUID);
                spatialMap.addTargetToBasesMapping(next.getAttribute(targetUUID).toString(), containsMap);
            }
        } finally {
            featureIt.close();
        }
        return spatialMap;
    }

    public static ArrayList<String> contains(SimpleFeature targetGranule, SimpleFeatureCollection baseGranules,
                                             String baseUUID) {
        ArrayList<String> nearestNeighbors = new ArrayList<>();
        SimpleFeatureIterator featureIt = baseGranules.features();
        while (featureIt.hasNext()) {
            SimpleFeature next = featureIt.next();
            Geometry baseGeometry = (Geometry) next.getDefaultGeometry();
            boolean contains = ((Geometry) targetGranule.getDefaultGeometry()).covers(baseGeometry);
            if (contains) {
                nearestNeighbors.add(next.getAttribute(baseUUID).toString());
            }
        }
        return nearestNeighbors;
    }

    public static Map<String, Object> getArguments() {
        return ARGUMENTS;
    }
}
