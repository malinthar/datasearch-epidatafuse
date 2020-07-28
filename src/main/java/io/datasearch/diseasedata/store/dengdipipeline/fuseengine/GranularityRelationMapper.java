package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import io.datasearch.diseasedata.store.query.QueryManager;
import io.datasearch.diseasedata.store.query.QueryObject;
import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
//import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.process.query.KNearestNeighborSearchProcess;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * For granularity Mapping.
 */
public class GranularityRelationMapper {

    private DataStore dataStore;
    private QueryManager queryManager;
    private static final Logger logger = LoggerFactory.getLogger(GranularityRelationMapper.class);

    private String baseSpatialGranularity;
    private String baseTemporalGranularity;
    private ArrayList<Map<String, Object>> spatioTemporalGranularityConfigs;

    private Map<String, GranularityMap> spatialGranularityMap;

    public GranularityRelationMapper(DataStore dataStore) {
        this.dataStore = dataStore;
        this.queryManager = new QueryManager();
        this.spatialGranularityMap = new HashMap<String, GranularityMap>();
    }

    public Map<String, GranularityMap> getSpatialGranularityMap() {
        return this.spatialGranularityMap;
    }

    public void buildGranularityMap() throws Exception {
        this.loadConfigs();

        for (Map<String, Object> configs : this.spatioTemporalGranularityConfigs) {

            String featureType = (String) configs.get("feature");
            Map<String, Object> spatialConfigs = (Map<String, Object>) configs.get("spatial");
            Map<String, Object> temporalConfigs = (Map<String, Object>) configs.get("temporal");

            this.buildSpatialMappingForFeatureType(featureType, spatialConfigs);
            this.buildTemporalMappingForFeatureType(featureType, temporalConfigs);
        }
    }

    public void buildSpatialMappingForFeatureType(String featureType,
                                                  Map<String, Object> spatialConfigs) throws Exception {

        SimpleFeatureCollection baseGranularityPoints = this.getGranularityFeatures(this.baseSpatialGranularity);
        SimpleFeatureCollection featureGranularityPoints = this.getGranularityFeatures(featureType);

        String mappingMethod = (String) spatialConfigs.get("spatialMapping");

        switch (mappingMethod) {
            case "nearest":
                NearestPointGranularityMap nearestPointMap = this.nearestPointAggregation(featureType,
                        baseGranularityPoints, featureGranularityPoints);

                nearestPointMap.getMap().forEach((String basePoint, ArrayList<String> nearestPoints) -> {
                    logger.info(basePoint + " " + nearestPoints.toString());
                });

                this.spatialGranularityMap.put(featureType, nearestPointMap);
                break;

            case "":
                // code block
                break;
            default:
                // code block
        }

    }

    public void buildTemporalMappingForFeatureType(String featureType, Map<String, Object> temporalConfigs) {

    }

    public void loadConfigs() {
        Map<String, Object> granularityMappings = ConfigurationLoader.getGranularityMappingConfigurations();
        Map<String, Object> baseGranularity = (Map<String, Object>) granularityMappings.get("baseGranularity");
        this.baseSpatialGranularity = (String) baseGranularity.get("spatial");
        this.baseTemporalGranularity = (String) baseGranularity.get("temporal");


        ArrayList<Map<String, Object>> mappings = (ArrayList<Map<String, Object>>) granularityMappings.get("mappings");
        this.spatioTemporalGranularityConfigs = mappings;
    }


    public SimpleFeatureCollection getGranularityFeatures(String featureName) throws Exception {
        Query query = new Query(featureName);
        QueryObject queryObj = new QueryObject("dengDIDataStore-test", query, featureName);
        ArrayList<SimpleFeature> featureList = this.queryManager.getFeatures(this.dataStore, queryObj);
        SimpleFeatureCollection featureCollection = DataUtilities.collection(featureList);
        return featureCollection;
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

    //this should be transferred to the aggregation method interface

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

}
