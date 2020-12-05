package io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.AttributeUtil;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
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

    /**
     * For each target granule find the nearest base granules based on a reference point.
     *
     * @param targetGranuleSet set of target granules
     * @param baseGranuleSet   set of base granules
     * @param neighbors        maximum number of nearest neighbors
     * @param maxDistance      maximum distance for a neighbor
     * @return a map of target granule and base granules
     */
    public static SpatialGranularityRelationMap buildNearestMap(SimpleFeatureCollection targetGranuleSet,
                                                                SimpleFeatureCollection baseGranuleSet,
                                                                int neighbors,
                                                                double maxDistance) {

        SimpleFeatureCollection targetGranuleSetAsPoints = convertGeometryToPoints(targetGranuleSet);
        SimpleFeatureCollection baseGranuleSetAsPoints = convertGeometryToPoints(baseGranuleSet);

        SpatialGranularityRelationMap spatialMap = new SpatialGranularityRelationMap();
//        SimpleFeatureIterator featureIt = targetGranuleSet.features();
        SimpleFeatureIterator featureIt = targetGranuleSetAsPoints.features();

        try {
            while (featureIt.hasNext()) {
                SimpleFeature targetPoint = featureIt.next();
                ArrayList<String> nearestNeighbors =
                        getNearestPoints(targetPoint, baseGranuleSetAsPoints, neighbors, maxDistance);
                spatialMap.addTargetToBasesMapping(targetPoint.getID(), nearestNeighbors);
                String msg =
                        targetPoint.getID() + " " + targetPoint.getAttribute(1).toString() + nearestNeighbors.size();
                logger.info(msg);
            }
        } finally {
            featureIt.close();
        }
        return spatialMap;
    }

    /**
     * Given a reference point returns the nearest neighbors.
     *
     * @param targetPoint
     * @param pointList
     * @param neighbors
     * @param maxDistance
     * @return
     */
    private static ArrayList<String> getNearestPoints(
            SimpleFeature targetPoint, SimpleFeatureCollection pointList, int neighbors, Double maxDistance) {

        ArrayList<String> nearestNeighbors = new ArrayList<>();
//        if(targetPoint.getDefaultGeometryProperty().getType() )
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

    private static SimpleFeatureCollection convertGeometryToPoints(SimpleFeatureCollection geomCollection) {
        ArrayList<SimpleFeature> pointCollectionArray = new ArrayList<SimpleFeature>();
        SimpleFeatureIterator featureIt = geomCollection.features();

        StringBuilder tempFeatureAttributes = new StringBuilder();
        tempFeatureAttributes.append(AttributeUtil.getAttribute("id", "String"));
        tempFeatureAttributes.append(",");
        tempFeatureAttributes.append(AttributeUtil.getAttribute("geom", "Point"));

        SimpleFeatureType tempSimpleFeatureType = SimpleFeatureTypes.createType("temptype",
                tempFeatureAttributes.toString());

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(tempSimpleFeatureType);

        while (featureIt.hasNext()) {
            SimpleFeature next = featureIt.next();
            Geometry geom = (Geometry) next.getDefaultGeometry();
            Point centroidPoint = geom.getCentroid();
            String featureID = next.getID();

            builder.add(featureID);
            builder.add(centroidPoint);

            SimpleFeature temp = builder.buildFeature(featureID);

            pointCollectionArray.add(temp);
        }
        SimpleFeatureCollection pointCollection = DataUtilities.collection(pointCollectionArray);
        return pointCollection;
    }
}
