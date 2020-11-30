package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * nearest mapping method
 */

public class NearestMapper {
    private static final Logger logger = LoggerFactory.getLogger(NearestMapper.class);
    public static final String MAPPER_NAME = "Nearest";
    public static final String ARG_NEIGHBORS = "neighbors";
    public static final String ARG_MAX_DISTANCE = "maxDistance";
    private static final Integer DEFAULT_NEIGHBORS = 1;
    private static final Integer DEFAULT_MAX_DISTANCE = 0;
    private static final Map<String, Object> ARGUMENTS = new HashMap<>();

    static {
        ARGUMENTS.put(ARG_NEIGHBORS, DEFAULT_NEIGHBORS);
        ARGUMENTS.put(ARG_MAX_DISTANCE, DEFAULT_MAX_DISTANCE);
    }

    public static SpatialGranularityRelationMap buildNearestMap(SimpleFeatureCollection targetGranuleSet,
                                                                SimpleFeatureCollection baseGranuleSet, int neighbors,
                                                                double maxDistance) {
        SpatialGranularityRelationMap spatialMap = new SpatialGranularityRelationMap();
        SimpleFeatureIterator featureIt = targetGranuleSet.features();

        try {
            while (featureIt.hasNext()) {
                SimpleFeature targetPoint = featureIt.next();
                ArrayList<String> nearestNeighbors = getNearestPoints(targetPoint, baseGranuleSet, neighbors,
                        maxDistance);

                spatialMap.addPoint(targetPoint.getID(), nearestNeighbors);
                String msg = targetPoint.getID() + nearestNeighbors.toString();
                logger.info(msg);
            }
        } finally {
            featureIt.close();
        }

        return spatialMap;
    }

    private static ArrayList<String> getNearestPoints(
            SimpleFeature targetPoint, SimpleFeatureCollection pointList, int neighbors, Double maxDistance) {

        ArrayList<String> nearestNeighbors = new ArrayList<>();

        SimpleFeatureCollection targetPointCollection = DataUtilities.collection(targetPoint);

        KNearestNeighborSearchProcess kNNProcess = new KNearestNeighborSearchProcess();
        SimpleFeatureCollection neighborPoints = kNNProcess.execute(targetPointCollection, pointList,
                neighbors, 50000.0, maxDistance);
        SimpleFeatureIterator featureIt = neighborPoints.features();

        try {
            while (featureIt.hasNext()) {
                SimpleFeature sf = featureIt.next();
                //logger.info(featureID + " : " + sf.getID());
                nearestNeighbors.add(sf.getID());
            }

            return nearestNeighbors;
        } finally {
            featureIt.close();
        }
    }

    public static Map<String, Object> getArguments() {
        return ARGUMENTS;
    }
}
