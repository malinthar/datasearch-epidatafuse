package io.datasearch.diseasedata.store.dengdipipeline.models.aggregationmethods;

import io.datasearch.diseasedata.store.dengdipipeline.models.granularitymappingmethods.NearestPointGranularityMap;
import io.datasearch.diseasedata.store.query.QueryManager;
import io.datasearch.diseasedata.store.query.QueryObject;
import org.geotools.data.DataStore;
import org.geotools.data.Query;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

/**
 *
 */

public class NearestPointsAggregator implements Aggregator {

    private static final Logger logger = LoggerFactory.getLogger(NearestPointsAggregator.class);
    private DataStore dataStore;

    public NearestPointsAggregator(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void nearestPointGranularityMapConvertor(String featureType, String valueAttribute,
                                                    String featureGranularityType,
                                                    String featureGranularityTypeIndexCol,
                                                    NearestPointGranularityMap spatialMap) throws Exception {

        String targerSpatialGranularity = "moh";
        QueryManager queryManager = new QueryManager();
        Query query = new Query(targerSpatialGranularity);

        QueryObject queryObj = new QueryObject("dengDIDataStore-test", query, targerSpatialGranularity);
        ArrayList<SimpleFeature> targetSpatialGranulaityFeatureList = queryManager.getFeatures(
                this.dataStore, queryObj);

//        String featureGranularityTypeIndexCol =  this.dataStore.getSchema(featureGranularityType).getDescriptor(0)
//                .getLocalName();

        for (SimpleFeature targetPoint : targetSpatialGranulaityFeatureList) {
            ArrayList<Float> neighborValues = new ArrayList<Float>();

            ArrayList<String> nearestPoints = spatialMap.getNearestPoints(targetPoint.getID());

            for (String nearestPoint : nearestPoints) {
                String filter = featureGranularityTypeIndexCol + "='" + nearestPoint + "'";
                query = new Query(featureType, ECQL.toFilter(filter));
                queryObj = new QueryObject("dengDIDataStore-test", query, featureGranularityType);


                ArrayList<SimpleFeature> features = queryManager.getFeatures(this.dataStore, queryObj);
                if (!features.isEmpty()) {
                    SimpleFeature feature = features.get(0);
                    String valueString = (String) feature.getAttribute(valueAttribute);

                    try {
                        float value = Float.parseFloat(valueString);
                        neighborValues.add(value);
                    } catch (Exception e) {
                        continue;
                    }
                }
            }

            float aggregatedValue = this.calculateFinalValue("max", neighborValues);

            logger.info(targetPoint.getID() + " " + aggregatedValue);

        }
    }

    public Float calculateFinalValue(String calculationMethod, ArrayList<Float> valueList) {
        switch (calculationMethod) {
            case "mean":
                float total = 0;

                for (float value : valueList) {
                    total = total + value;
                }

                float mean = total / valueList.size();

                return mean;
            case "max":
                float max = Collections.max(valueList);
                return max;

            case "min":
                float min = Collections.min(valueList);
                return min;

            default:
                return null;
        }
    }
}
