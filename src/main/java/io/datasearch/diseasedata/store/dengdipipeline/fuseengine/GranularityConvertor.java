package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import io.datasearch.diseasedata.store.query.QueryManager;
import io.datasearch.diseasedata.store.query.QueryObject;
import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * For granularity conversion.
 */
public class GranularityConvertor {
    private static final Logger logger = LoggerFactory.getLogger(GranularityConvertor.class);

    private DataStore dataStore;

    public GranularityConvertor(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public List<Map<String, Object>> loadFeatureGranularities() {
        try {
            Map<String, Object> granularityConfigurations = ConfigurationLoader.getGranularityConfigurations();

            List<Map<String, Object>> featuregranularities =
                    (ArrayList) granularityConfigurations.get("feature_granularities");

            //for (Map<String, Object> featuregranularity : featuregranularities) {
            //logger.info("Reading Granularities: " + featuregranularity.toString() + "\n");
            //}

            return featuregranularities;

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error reading granularity config:", e);
        }
    }

    public Map<String, Object> loadAggrigationGranularities() {
        try {
            Map<String, Object> granularityConfigurations = ConfigurationLoader.getGranularityConfigurations();

            Map<String, Object> aggrigategranularities =
                    (Map<String, Object>) granularityConfigurations.get("aggregation_granularities");

            //for (Map<String, Object> featuregranularity : featuregranularities) {
            //logger.info("Reading Granularities: " + featuregranularity.toString() + "\n");
            //}

            return aggrigategranularities;

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error reading granularity config:", e);
        }
    }

    public void temporalConversion() {
    }

//    public String getSpatialGranularity(String featureType) {
//        return "weatherstations";
//    }

    public void nearestPointGranularityMapConvertor(String featureType, String valueAttribute,
                                                    String originalGranularityFeature,
                                                    String originalGranularityIndexName,
                                                    NearestPointGranularityMap spatialMap) throws Exception {

        String targerSpatialGranularity = "moh";
        QueryManager queryManager = new QueryManager();
        Query query = new Query(targerSpatialGranularity);

        QueryObject queryObj = new QueryObject("dengDIDataStore-test", query, targerSpatialGranularity);
        ArrayList<SimpleFeature> targetSpatialGranulaityFeatureList = queryManager.getFeatures(
                this.dataStore, queryObj);

        for (SimpleFeature targetPoint : targetSpatialGranulaityFeatureList) {
            String targetValue = "";
            int count = 0;
            ArrayList<String> nearestPoints = spatialMap.getNearestPoints(targetPoint.getID());
            for (String nearestPoint : nearestPoints) {
                String filter = originalGranularityIndexName + "='" + nearestPoint + "'";
                query = new Query(featureType, ECQL.toFilter(filter));
                queryObj = new QueryObject("dengDIDataStore-test", query, originalGranularityFeature);

                ArrayList<SimpleFeature> features = queryManager.getFeatures(this.dataStore, queryObj);
                if (!features.isEmpty()) {
                    SimpleFeature feature = features.get(0);
                    String value = (String) feature.getAttribute(valueAttribute);

                    targetValue = targetValue + " " + value;
                    count++;
                }
            }

            logger.info(targetPoint.getID() + " " + targetValue);

        }


    }
}
