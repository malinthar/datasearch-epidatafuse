package io.datasearch.diseasedata.store.dengdipipeline.models.spatialrelationsmapper;

import io.datasearch.diseasedata.store.dengdipipeline.models.granularitymappingmethods.NearestPointGranularityMap;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *
 */
public class NearestPointRelationMapper extends SpatialRelationsMapper {

    private static final Logger logger = LoggerFactory.getLogger(NearestPointRelationMapper.class);

    public NearestPointGranularityMap nearestPointAggregation(
            String featureType, SimpleFeatureCollection baseGranularityPoints,
            SimpleFeatureCollection featureGranularityPoints) {

        //ArrayList<Map<String, ArrayList<String>>> spatialMapList = new ArrayList<Map<String, ArrayList<String>>>();
        NearestPointGranularityMap nearestPointMap = new NearestPointGranularityMap(featureType);

        SimpleFeatureIterator it = baseGranularityPoints.features();
        while (it.hasNext()) {
            SimpleFeature point = it.next();
            SimpleFeatureCollection singlePoint = DataUtilities.collection(point);
            ArrayList<String> nearestPoints = this.getNearestPoint(singlePoint,
                    featureGranularityPoints, 3);

            nearestPointMap.addPoint(point.getID(), nearestPoints);

            //logger.info(featureType + " " + singlePointMap.toString());
        }

        //logger.info(spatialMap.toString());

        return nearestPointMap;
    }

    public ArrayList<String> getNearestPoint(
            SimpleFeatureCollection pointA, SimpleFeatureCollection pointList, int neighbors) {
        try {

            //Map<String, ArrayList<String>> spatialPointsMap = new HashMap<String, ArrayList<String>>();
            ArrayList<String> nearestNeighbors = new ArrayList<>();

            KNearestNeighborSearchProcess kNNProcess = new KNearestNeighborSearchProcess();
            SimpleFeatureCollection neighborPoints = kNNProcess.execute(pointA, pointList,
                    neighbors, 50000.0, 10000000.0);
            SimpleFeatureIterator featureIt = neighborPoints.features();
            while (featureIt.hasNext()) {
                SimpleFeature sf = featureIt.next();
                //logger.info(featureID + " : " + sf.getID());
                nearestNeighbors.add(sf.getID());
            }

            return nearestNeighbors;

        } catch (Exception e) {
            logger.info(e.getMessage());
            return null;
        }
    }

}
