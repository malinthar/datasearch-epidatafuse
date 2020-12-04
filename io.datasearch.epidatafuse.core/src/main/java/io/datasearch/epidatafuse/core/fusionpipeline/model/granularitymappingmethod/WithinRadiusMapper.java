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
 * Mapping granules within a given radius
 */
public class WithinRadiusMapper {
    public static final String MAPPER_NAME = "WithinRadius";
    private static final Map<String, Object> ARGUMENTS = new HashMap<>();
    public static final String ARG_RADIUS = "radius";
    public static final String ARG_MAX_NEIGHBORS = "maxDistance";
    private static final Integer DEFAULT_RADIUS = 1000;
    private static final Integer DEFAULT_MAX_NEIGHBORS = 0;

    static {
        ARGUMENTS.put(ARG_RADIUS, DEFAULT_RADIUS);
        ARGUMENTS.put(ARG_MAX_NEIGHBORS, DEFAULT_MAX_NEIGHBORS);
    }

    /**
     * Find the points with in the neighborhood of radius 'R' from a reference point.
     *
     * @param targetGranuleSet
     * @param baseGranuleSet
     * @return
     */
    public static SpatialGranularityRelationMap buildWithinRadiusMap(SimpleFeatureCollection targetGranuleSet,
                                                                     SimpleFeatureCollection baseGranuleSet) {
        SpatialGranularityRelationMap spatialMap = new SpatialGranularityRelationMap();
        SimpleFeatureIterator featureIt = targetGranuleSet.features();
        try {
            while (featureIt.hasNext()) {
                SimpleFeature next = featureIt.next();
                ArrayList<String> withinRadiusList = findWithInRadius(next, baseGranuleSet);
                spatialMap.addTargetToBasesMapping(next.getID(), withinRadiusList);
            }
        } finally {
            featureIt.close();
        }
        return spatialMap;
    }

    public static ArrayList<String> findWithInRadius(SimpleFeature targetGranule,
                                                     SimpleFeatureCollection baseGranules) {
        ArrayList<String> nearestNeighbors = new ArrayList<>();
        SimpleFeatureIterator featureIt = baseGranules.features();
        while (featureIt.hasNext()) {
            SimpleFeature next = featureIt.next();
            Geometry baseGeometry = (Geometry) next.getDefaultGeometry();
            boolean contains = ((Geometry) targetGranule.getDefaultGeometry()).
                    isWithinDistance(baseGeometry, DEFAULT_RADIUS);
            if (contains) {
                nearestNeighbors.add(next.getID());
            }
        }
        return nearestNeighbors;
    }

    public static Map<String, Object> getArguments() {
        return ARGUMENTS;
    }
}
