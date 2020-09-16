package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import io.datasearch.diseasedata.store.dengdipipeline.models.aggregationmethods.AggregateInvoker;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.AggregationConfig;
import io.datasearch.diseasedata.store.dengdipipeline.models.granularityrelationmap.GranularityMap;
import io.datasearch.diseasedata.store.dengdipipeline.models.granularityrelationmap.SpatialGranularityRelationMap;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * For granularity conversion.
 */
public class GranularityConvertor {
    private static final Logger logger = LoggerFactory.getLogger(GranularityConvertor.class);

    private DataStore dataStore;

    public GranularityConvertor(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void aggregate(GranularityMap granularityMap, AggregationConfig config) throws IOException {

        String featureTypeName = config.getFeatureTypeName();
        String targetSpatialGranularity = granularityMap.getTargetSpatialGranularity();

        SimpleFeatureCollection targetGranuleSet = this.getFeatures(targetSpatialGranularity);
        SimpleFeatureCollection featureSet = this.getFeatures(featureTypeName);

        String indexCol = config.getIndexCol();
        String aggregateOn = config.getAggregationOn();
        String aggregationMethod = config.getAggregationMethod();
        String aggregationType = config.getAggregationType();

        this.spatialAggregate(targetGranuleSet, featureSet, indexCol, aggregateOn,
                granularityMap.getSpatialGranularityRelationMap(), aggregationType, aggregationMethod);
    }

    public void spatialAggregate(SimpleFeatureCollection targetGranuleSet, SimpleFeatureCollection featureSet,
                                 String indexCol, String aggregateOn,
                                 SpatialGranularityRelationMap spatialGranularityMap, String aggregationType,
                                 String aggregationMethod) {

        SimpleFeatureIterator iterator = targetGranuleSet.features();

        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            String targetGranule = feature.getID();

            ArrayList<String> baseGranuleIds = spatialGranularityMap.getBasePointIds(targetGranule);
            HashMap<String, Double> valueSet = this.getAggregatingAttributes(baseGranuleIds, featureSet, aggregateOn);

            Double aggregatedValue = this.calculateFinalValue(valueSet, aggregationType, aggregationMethod);
            logger.info(feature.getID() + " " + aggregatedValue.toString());
        }
    }


    //given the corresponding base granule ids and feature set get the corresponding value set.
    public HashMap<String, Double> getAggregatingAttributes(ArrayList<String> granuleIds,
                                                            SimpleFeatureCollection featureCollection,
                                                            String attributeCol) {


        HashMap<String, Double> valueSet = new HashMap<String, Double>();
        HashMap<String, SimpleFeature> featureSet = new HashMap<String, SimpleFeature>();

        SimpleFeatureIterator iterator = featureCollection.features();
        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            featureSet.put(feature.getID(), feature);
        }

        granuleIds.forEach((granule) -> {
            try {
                SimpleFeature feature = featureSet.get(granule);
                String valueString = (String) feature.getAttribute(attributeCol);
                Double value = Double.parseDouble(valueString);
                valueSet.put(granule, value);
            } catch (Exception e) {
                valueSet.put(granule, null);
            }
        });

        //logger.info(valueSet.toString());
        return valueSet;
    }

    private Double calculateFinalValue(HashMap<String, Double> valueSet, String aggregationType,
                                       String aggregationMethod) {
        Double finalValue;

        if (aggregationType.equals("aggregation")) {
            switch (aggregationMethod) {
                case "mean":
                    finalValue = AggregateInvoker.mean(valueSet);
                    break;
                default:
                    finalValue = -0.4;
                    break;
            }

        } else if (aggregationType.equals("interpolation")) {
            finalValue = -0.05;
        } else {
            finalValue = -0.002;
        }
        return finalValue;
    }

    //get features from the datastore
    public SimpleFeatureCollection getFeatures(String typeName) throws IOException {
        Query query = new Query(typeName);

        FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                this.dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

        ArrayList<SimpleFeature> featureList = new ArrayList<SimpleFeature>();
        while (reader.hasNext()) {
            SimpleFeature feature = (SimpleFeature) reader.next();
            featureList.add(feature);
        }
        reader.close();
        SimpleFeatureCollection featureCollection = DataUtilities.collection(featureList);
        return featureCollection;
    }
}
