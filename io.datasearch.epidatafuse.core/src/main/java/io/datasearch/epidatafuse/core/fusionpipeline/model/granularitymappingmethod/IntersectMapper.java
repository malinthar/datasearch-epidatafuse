package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * intersect object mapping method.
 */
public class IntersectMapper {
    private static final Logger logger = LoggerFactory.getLogger(IntersectMapper.class);
    public static final String MAPPER_NAME = "Intersect";
    private static final Map<String, Object> ARGUMENTS = new HashMap<>();

    public static SpatialGranularityRelationMap buildIntersectMap(SimpleFeatureCollection targetGranuleSet,
                                                                SimpleFeatureCollection baseGranuleSet, String baseUUID,
                                                                String targetUUID) {
        SpatialGranularityRelationMap spatialMap = new SpatialGranularityRelationMap();
        SimpleFeatureIterator featureIt = targetGranuleSet.features();
        try {
            while (featureIt.hasNext()) {
                SimpleFeature next = featureIt.next();
                ArrayList<String> intersectMap = intersect(next, baseGranuleSet, baseUUID);
                spatialMap.addTargetToBasesMapping(next.getAttribute(targetUUID).toString(), intersectMap);
                String msg = next.getAttribute(targetUUID) + " " + intersectMap.size();
                logger.info(msg);
            }
        } finally {
            featureIt.close();
        }
        return spatialMap;
    }

    public static ArrayList<String> intersect(SimpleFeature targetGranule, SimpleFeatureCollection baseGranules,
                                             String baseUUID) {
        ArrayList<String> intersectNeighbors = new ArrayList<>();
        SimpleFeatureIterator featureIt = baseGranules.features();
        while (featureIt.hasNext()) {
            SimpleFeature next = featureIt.next();
            Geometry baseGeometry = (Geometry) next.getDefaultGeometry();
            boolean intersects = ((Geometry) targetGranule.getDefaultGeometry()).intersects(baseGeometry);
            if (intersects) {
                intersectNeighbors.add(next.getAttribute(baseUUID).toString());
            }
        }
        return intersectNeighbors;
    }

    public static Map<String, Object> getArguments() {
        return ARGUMENTS;
    }
}
