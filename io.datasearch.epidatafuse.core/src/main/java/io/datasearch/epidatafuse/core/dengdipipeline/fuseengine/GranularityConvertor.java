package io.datasearch.epidatafuse.core.dengdipipeline.fuseengine;

import io.datasearch.epidatafuse.core.dengdipipeline.models.aggregationmethods.AggregateInvoker;
import io.datasearch.epidatafuse.core.dengdipipeline.models.configmodels.AggregationConfig;
import io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels.SpatioTemporallyAggregatedCollection;
import io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels.TemporallyAggregatedCollection;
import io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap.GranularityMap;
import io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap.SpatialGranularityRelationMap;
import io.datasearch.epidatafuse.core.dengdipipeline.models.granularityrelationmap.TemporalGranularityMap;


import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
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

    public SpatioTemporallyAggregatedCollection aggregate(GranularityMap granularityMap, AggregationConfig config)
            throws IOException {


        String baseSpatialGranularity = granularityMap.getBaseSpatialGranularity();
        String targetSpatialGranularity = granularityMap.getTargetSpatialGranularity();

        SimpleFeatureCollection baseSpatialGranuleSet = this.getFeatures(baseSpatialGranularity);
        SimpleFeatureCollection targetSpatialGranuleSet = this.getFeatures(targetSpatialGranularity);

        //features are taken directly from the database without temporal aggregation
        //SimpleFeatureCollection featureSet = this.getFeatures(featureTypeName);

        String indexCol = config.getIndexCol();
        String aggregateOn = config.getAggregationOn();
        String aggregationMethod = config.getSpatialAggregationMethod();
        Boolean isASpatialInterpolation = config.isASpatialInterpolation();

        TemporallyAggregatedCollection temporallyAggregatedFeatures =
                this.temporalAggregate(granularityMap, config, baseSpatialGranuleSet);


        SpatioTemporallyAggregatedCollection spatioTemporallyAggregatedCollection =
                this.spatialAggregate(targetSpatialGranuleSet, temporallyAggregatedFeatures, indexCol, aggregateOn,
                        granularityMap, isASpatialInterpolation, aggregationMethod);

        return spatioTemporallyAggregatedCollection;
    }

    public TemporallyAggregatedCollection temporalAggregate(GranularityMap granularityMap,
                                                            AggregationConfig config,
                                                            SimpleFeatureCollection baseSpatialGranuleSet) {

        TemporalGranularityMap temporalGranularityMap = granularityMap.getTemporalGranularityMap();

        String featureTypeName = config.getFeatureTypeName();
        String indexCol = config.getIndexCol();
        String aggregateOn = config.getAggregationOn();
        String aggregationMethod = config.getTemporalAggregationMethod();
        Boolean isATemporalInterpolation = config.isATemporalInterpolation();

        String baseSpatialGranularity = granularityMap.getBaseSpatialGranularity();

        String targetTemporalGranularity = temporalGranularityMap.getTargetTemporalGranularity();
        long relationValue = temporalGranularityMap.getRelationValue();

//        LocalDateTime currentTimestamp = LocalDateTime.now();
        LocalDateTime currentTimestamp = LocalDateTime.of(2013, 1, 4, 8, 00, 00, 00);
        LocalDateTime startingTimestamp = currentTimestamp.minusHours(relationValue);

        logger.info(currentTimestamp.toString());
        logger.info(startingTimestamp.toString());

        SimpleFeatureIterator iterator = baseSpatialGranuleSet.features();

        SimpleFeatureType featureType = this.getFeatureType(featureTypeName);

        ArrayList<SimpleFeature> aggregatedFeatures = new ArrayList<SimpleFeature>();

        while (iterator.hasNext()) {

            SimpleFeature baseSpatialGranule = iterator.next();
            String baseSpatialGranuleID = baseSpatialGranule.getID();

            ArrayList<SimpleFeature> featuresToAggregate =
                    this.getFeaturesBetweenDates(featureTypeName, startingTimestamp.toString(),
                            currentTimestamp.toString(), indexCol, baseSpatialGranuleID);

            if (featuresToAggregate.size() > 0) {

                HashMap<String, Double> valueSet = new HashMap<String, Double>();
                SimpleFeature aggregatedFeature = null;

                for (SimpleFeature feature : featuresToAggregate) {

                    String dtg = feature.getAttribute("dtg").toString();
                    aggregatedFeature = feature;
                    try {
                        Double value = Double.parseDouble(feature.getAttribute(aggregateOn).toString());
                        valueSet.put(dtg, value);
                    } catch (Exception e) {
                        continue;
                    }
                }

                Double aggregatedValue = this.calculateFinalValue(valueSet, isATemporalInterpolation,
                        aggregationMethod, null);

                aggregatedFeature.setAttribute(aggregateOn, aggregatedValue);
                logger.info(aggregatedFeature.toString());
                aggregatedFeatures.add(aggregatedFeature);
            }
        }

        SimpleFeatureCollection aggregatedFeatureCollection = DataUtilities.collection(aggregatedFeatures);

        TemporallyAggregatedCollection temporallyAggregatedCollection =
                new TemporallyAggregatedCollection(
                        featureType,
                        aggregatedFeatureCollection,
                        baseSpatialGranularity,
                        targetTemporalGranularity,
                        currentTimestamp.toString()
                );

        return temporallyAggregatedCollection;
    }

    public SpatioTemporallyAggregatedCollection spatialAggregate(
            SimpleFeatureCollection targetGranuleSet,
            TemporallyAggregatedCollection temporallyAggregatedfeatureSet,
            String indexCol, String aggregateOn,
            GranularityMap granularityMap,
            Boolean isASpatialInterpolation,
            String aggregationMethod
    ) {

        SimpleFeatureType featureType = temporallyAggregatedfeatureSet.getFeatureType();
        SimpleFeatureCollection featureSet = temporallyAggregatedfeatureSet.getFeatureCollection();
        String dtg = temporallyAggregatedfeatureSet.getDtg();
        SpatialGranularityRelationMap spatialGranularityMap = granularityMap.getSpatialGranularityRelationMap();


        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        ArrayList<SimpleFeature> aggregatedFeatures = new ArrayList<SimpleFeature>();

        SimpleFeatureIterator iterator = targetGranuleSet.features();

        String formatedDtgString = dtg;

        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            String targetGranule = feature.getID();

            //corresponding granule ids for the target according to granularityMap
            ArrayList<String> baseGranuleIds = spatialGranularityMap.getBasePointIds(targetGranule);

            //get the observed or recorded values of each corresponding base granule
            HashMap<String, Double> valueSet = this.getAggregatingAttributes(baseGranuleIds, featureSet, aggregateOn);

            //get the required custom attributes such as weighting factors for aggregation
            HashMap<String, Double> customAttributeSet =
                    this.getCustomAttributes(baseGranuleIds, targetGranule, aggregationMethod);

            Double aggregatedValue =
                    this.calculateFinalValue(valueSet, isASpatialInterpolation, aggregationMethod, customAttributeSet);

            //logger.info(targetGranule + " " + aggregatedValue.toString() + " " + dtg);

            SimpleFeature aggregatedFeature = featureBuilder.buildFeature(targetGranule);
            aggregatedFeature.setAttribute(aggregateOn, aggregatedValue);

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
            DateFormat dateStringFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

            Date date;

            try {
                date = format.parse(dtg);
                formatedDtgString = dateStringFormat.format(date);
            } catch (Exception e) {
                e.getMessage();
                date = null;
                formatedDtgString = null;
            }
            aggregatedFeature.setAttribute("dtg", date);
            logger.info(aggregatedFeature.toString());

            aggregatedFeatures.add(aggregatedFeature);
        }

        ArrayList<String> attributeList = new ArrayList<String>();
        attributeList.add(indexCol);
        attributeList.add(aggregateOn);
        attributeList.add("dtg");

        SimpleFeatureCollection aggregatedFeatureCollection = DataUtilities.collection(aggregatedFeatures);

        SpatioTemporallyAggregatedCollection spatioTemporallyAggregatedCollection =
                new SpatioTemporallyAggregatedCollection(
                        featureType,
                        aggregatedFeatureCollection,
                        granularityMap.getTargetSpatialGranularity(),
                        granularityMap.getTargetTemporalGranularity(),
                        formatedDtgString,
                        attributeList
                );
        return spatioTemporallyAggregatedCollection;
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

    private HashMap<String, Double> getCustomAttributes(ArrayList<String> baseGranuleIds, String targetGranule,
                                                        String aggregationMethod) {

        HashMap<String, Double> customAttributes = new HashMap<String, Double>();

        switch (aggregationMethod) {
            case "inverseDistance":

                //caluculate distance from the target granule to each base granules.
                for (String baseGranuleId : baseGranuleIds) {

                    Double distance = this.calculateDistance(baseGranuleId, targetGranule);
                    customAttributes.put(baseGranuleId, distance);
                }
                break;
            default:
                break;

        }

        return customAttributes;
    }

    private Double calculateDistance(String baseGranuleId, String targetGranule) {
        //Pointorg.locationtech.geomesa.process.analytic.Point2PointProcess()
        return 0.0;
    }

    private Double calculateFinalValue(HashMap<String, Double> valueSet, Boolean isAnAggregate,
                                       String aggregationMethod, HashMap<String, Double> customAttributes) {
        Double finalValue;

        //if an aggregation process. there are two types, aggregation vs interpolation.
        if (!isAnAggregate) {
            switch (aggregationMethod) {
                case "mean":
                    finalValue = AggregateInvoker.mean(valueSet);
                    break;
                case "sum":
                    finalValue = AggregateInvoker.sum(valueSet);
                    break;
                case "max":
                    finalValue = AggregateInvoker.max(valueSet);
                    break;
                case "min":
                    finalValue = AggregateInvoker.min(valueSet);
                    break;
                case "inverseDistance":
                    finalValue = AggregateInvoker.inverseDistance(valueSet, customAttributes);
                    break;
                default:
                    finalValue = -0.4;
                    break;
            }

        } else if (isAnAggregate) {
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


    public ArrayList<SimpleFeature> getFeaturesBetweenDates(String typeName, String startingDate, String endDate,
                                                            String indexCol,
                                                            String distinctID) {
        try {
            Filter filter = CQL.toFilter(
                    indexCol + "='" + distinctID +
                            "' AND dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");

//            Filter filter = CQL.toFilter("dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");
            Query query = new Query(typeName, filter);
            //query.getHints().put(QueryHints.EXACT_COUNT(), Boolean.TRUE);

            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    this.dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            ArrayList<SimpleFeature> featureList = new ArrayList<SimpleFeature>();

            while (reader.hasNext()) {
                SimpleFeature feature = (SimpleFeature) reader.next();
                featureList.add(feature);
            }

            reader.close();
            return featureList;

        } catch (Exception e) {
            return null;
        }
    }

    public SimpleFeatureType getFeatureType(String featureTypeName) {
        try {
            SimpleFeatureType featureType = this.dataStore.getSchema(featureTypeName);
            return featureType;
        } catch (Exception e) {
            return null;
        }
    }
}
