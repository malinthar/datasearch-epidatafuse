package io.datasearch.epidatafuse.core.fusionpipeline.fuseengine;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.PipelineDataStore;
import io.datasearch.epidatafuse.core.fusionpipeline.model.aggregationmethod.AggregateInvoker;
import io.datasearch.epidatafuse.core.fusionpipeline.model.aggregationmethod.AggregationUtil;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.AggregationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.datamodel.SpatioTemporallyAggregatedCollection;
import io.datasearch.epidatafuse.core.fusionpipeline.model.datamodel.TemporallyAggregatedCollection;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.GranularityMap;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.SpatialGranularityRelationMap;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.TemporalGranularityMap;

import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.type.AttributeTypeImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
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
import java.util.List;
import java.util.Locale;

/**
 * For granularity conversion.
 */
public class GranularityConvertor {
    private static final Logger logger = LoggerFactory.getLogger(GranularityConvertor.class);
    private static final String INDEX_COLUMN_KEY = "spatialGranule";

    private PipelineDataStore pipelineDataStore;
    private DataStore dataStore;

    public GranularityConvertor(PipelineDataStore pipelineDataStore) {
        this.pipelineDataStore = pipelineDataStore;
        this.dataStore = pipelineDataStore.getDataStore();
    }

    public SpatioTemporallyAggregatedCollection aggregate(GranularityMap granularityMap, AggregationConfig config)
            throws IOException {


        String baseSpatialGranularity = granularityMap.getBaseSpatialGranularity();
        String targetSpatialGranularity = granularityMap.getTargetSpatialGranularity();

        SimpleFeatureCollection baseSpatialGranuleSet = this.getFeatures(baseSpatialGranularity);
        SimpleFeatureCollection targetSpatialGranuleSet = this.getFeatures(targetSpatialGranularity);

        //features are taken directly from the database without temporal aggregation
        //SimpleFeatureCollection featureSet = this.getFeatures(featureTypeName);

        String indexCol = this.INDEX_COLUMN_KEY;
        String aggregateOn = config.getAggregationOn();
        String aggregationMethod = config.getSpatialAggregationMethod(aggregateOn);
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
//        String indexCol = config.getIndexCol();
        String indexCol = this.INDEX_COLUMN_KEY;
        String aggregateOn = config.getAggregationOn();
        //todo: make this compatible for multiple attributes
        String aggregationMethod = config.getTemporalAggregationMethod(aggregateOn);
        Boolean isATemporalInterpolation = config.isATemporalInterpolation();

        String baseSpatialGranularity = granularityMap.getBaseSpatialGranularity();

        String baseSpatialUuid =
                this.pipelineDataStore.getGranularitySchema(baseSpatialGranularity).getUuidAttributeName();

        String targetTemporalGranularity = temporalGranularityMap.getTargetTemporalGranularity();
        long relationValue = temporalGranularityMap.getRelationValue();

//      LocalDateTime currentTimestamp = LocalDateTime.now();
        LocalDateTime currentTimestamp = LocalDateTime.of(2013, 1, 7, 8, 00, 00, 00);
        LocalDateTime startingTimestamp = currentTimestamp.minusHours(relationValue);

        logger.info(currentTimestamp.toString());
        logger.info(startingTimestamp.toString());

        SimpleFeatureIterator iterator = baseSpatialGranuleSet.features();

        SimpleFeatureType featureType = this.getFeatureType(featureTypeName);

        ArrayList<SimpleFeature> aggregatedFeatures = new ArrayList<SimpleFeature>();

        while (iterator.hasNext()) {

            SimpleFeature baseSpatialGranule = iterator.next();
            String baseSpatialGranuleID = baseSpatialGranule.getAttribute(baseSpatialUuid).toString();

            ArrayList<SimpleFeature> featuresToAggregate =
                    this.getFeaturesBetweenDates(featureTypeName, startingTimestamp.toString(),
                            currentTimestamp.toString(), baseSpatialUuid, baseSpatialGranuleID);

            if (featuresToAggregate.size() > 0) {

                HashMap<String, Double> valueSet = new HashMap<String, Double>();
                SimpleFeature aggregatedFeature = null;

                for (SimpleFeature feature : featuresToAggregate) {

                    String dtg = feature.getAttribute("dtg").toString();
                    aggregatedFeature = feature;
                    try {
                        Double value = Double.parseDouble(feature.getAttribute(aggregateOn).toString());
                        valueSet.put(dtg, value);
                    } catch (Throwable e) {
                        logger.error(e.getMessage());
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


        String targetSpatialGranularity = granularityMap.getTargetSpatialGranularity();
        String baseSpatialGranularity = granularityMap.getBaseSpatialGranularity();
        String targetUUID =
                this.pipelineDataStore.getGranularitySchema(targetSpatialGranularity).getUuidAttributeName();
//        String baseUUID = this.pipelineDataStore.getGranularitySchema(baseSpatialGranularity).getUuidAttributeName();

        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            String targetGranule = feature.getAttribute(targetUUID).toString();

            //corresponding granule ids for the target according to granularityMap
            ArrayList<String> baseGranuleIds = spatialGranularityMap.getBaseGranuleIds(targetGranule);

            //get the observed or recorded values of each corresponding base granule
            HashMap<String, Double> valueSet =
                    this.getAggregatingAttributes(baseSpatialGranularity, baseGranuleIds, featureSet, aggregateOn);

            //get the required custom attributes such as weighting factors for aggregation
            HashMap<String, Double> customAttributeSet =
                    this.getCustomAttributes(granularityMap.getBaseSpatialGranularity(), baseGranuleIds,
                            granularityMap.getTargetSpatialGranularity(), targetGranule, aggregationMethod);

            Double aggregatedValue =
                    this.calculateFinalValue(valueSet, isASpatialInterpolation, aggregationMethod, customAttributeSet);

            String info = (targetGranule + " " + aggregatedValue.toString() + " " + dtg);
            logger.info(info);

            SimpleFeature aggregatedFeature = featureBuilder.buildFeature(targetGranule);
            aggregatedFeature.setAttribute(aggregateOn, aggregatedValue);

            DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
            DateFormat dateStringFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

            Date date;

            try {
                date = format.parse(dtg);
                formatedDtgString = dateStringFormat.format(date);
            } catch (Throwable e) {
                logger.error(e.getMessage());
                date = null;
                formatedDtgString = null;
            }
            aggregatedFeature.setAttribute("dtg", date);
//            logger.info(aggregatedFeature.toString());

            aggregatedFeatures.add(aggregatedFeature);
        }

        ArrayList<String> attributeList = new ArrayList<String>();
        attributeList.add(indexCol);
        attributeList.add(aggregateOn);
        attributeList.add("dtg");

        ArrayList<String> aggregatedAttributeList = new ArrayList<String>();
        aggregatedAttributeList.add(aggregateOn);

        SimpleFeatureCollection aggregatedFeatureCollection = DataUtilities.collection(aggregatedFeatures);

        SpatioTemporallyAggregatedCollection spatioTemporallyAggregatedCollection =
                new SpatioTemporallyAggregatedCollection(
                        featureType,
                        aggregatedFeatureCollection,
                        granularityMap.getTargetSpatialGranularity(),
                        granularityMap.getTargetTemporalGranularity(),
                        formatedDtgString,
                        attributeList,
                        aggregatedAttributeList
                );
        return spatioTemporallyAggregatedCollection;
    }


    //given the corresponding base granule ids and feature set get the corresponding value set.
    public HashMap<String, Double> getAggregatingAttributes(String granularity, ArrayList<String> granuleIds,
                                                            SimpleFeatureCollection featureCollection,
                                                            String attributeCol) {


        String uuid = this.pipelineDataStore.getGranularitySchema(granularity).getUuidAttributeName();

        HashMap<String, Double> valueSet = new HashMap<String, Double>();
        HashMap<String, SimpleFeature> featureSet = new HashMap<String, SimpleFeature>();

        SimpleFeatureIterator iterator = featureCollection.features();
        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();
            featureSet.put(feature.getAttribute(uuid).toString(), feature);
        }

        granuleIds.forEach((granule) -> {
            try {
                // check whether granule is in the featureset
                if (featureSet.get(granule) != null) {
                    SimpleFeature feature = featureSet.get(granule);
                    String valueString = feature.getAttribute(attributeCol).toString();
                    Double value = Double.parseDouble(valueString);
                    valueSet.put(granule, value);
                }
            } catch (Exception e) {
                valueSet.put(granule, null);
            }
        });

        iterator.close();
        return valueSet;
    }

    private HashMap<String, Double> getCustomAttributes(String baseGranularity, ArrayList<String> baseGranuleIds,
                                                        String targetGranularity, String targetGranule,
                                                        String aggregationMethod) {

        HashMap<String, Double> customAttributes = new HashMap<String, Double>();
        ArrayList<SimpleFeature> baseGranules = new ArrayList<SimpleFeature>();
        SimpleFeature targetGranuleFeature = null;

        String baseGranularityIndexCol =
                this.pipelineDataStore.getGranularitySchema(baseGranularity).getUuidAttributeName();
        String targetGranularityIndexCol =
                this.pipelineDataStore.getGranularitySchema(targetGranularity).getUuidAttributeName();

//        String baseGranularityIndexCol = getFeatureIndexColName(baseGranularity);
//        String targetGranularityIndexCol = getFeatureIndexColName(targetGranularity);

        try {

            for (String baseGranuleId : baseGranuleIds) {

                Filter filter = CQL.toFilter(baseGranularityIndexCol + " = '" + baseGranuleId + "'");
                Query query = new Query(baseGranularity, filter);

                FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                        this.dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);


                while (reader.hasNext()) {
                    SimpleFeature sf = reader.next();
                    baseGranules.add(sf);
                }
            }

            Filter filter = CQL.toFilter(targetGranularityIndexCol + " = '" + targetGranule + "'");
            Query query = new Query(targetGranularity, filter);

            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    this.dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            while (reader.hasNext()) {
                SimpleFeature sf = reader.next();
                targetGranuleFeature = sf;
            }

            reader.close();

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
//        logger.info(targetGranuleFeature.toString());
//        logger.info(baseGranules.toString());

        switch (aggregationMethod) {
            case "InverseDistance":

                //caluculate distance from the target granule to each base granules.
                for (SimpleFeature baseGranule : baseGranules) {

                    Double distance = this.calculateDistance(baseGranule, targetGranuleFeature);
//                   String info = targetGranuleFeature.getID() + "-" + baseGranule.getID() + ":" + distance.toString();

                    customAttributes.put(baseGranule.getAttribute(baseGranularityIndexCol).toString(), distance);
                }
                break;

            case "AreaBasedAverage":
                for (SimpleFeature baseGranule : baseGranules) {
                    Double intersectRatio = this.calculateIntersectRatio(targetGranuleFeature, baseGranule);
                    customAttributes.put(baseGranule.getAttribute(baseGranularityIndexCol).toString(), intersectRatio);
                }
                break;
            default:
                break;

        }

        return customAttributes;
    }

    private Double calculateDistance(SimpleFeature granule1, SimpleFeature granule2) {
//        GeodeticCalculator geodeticCalculator = new GeodeticCalculator();
        //geodeticCalculator.setStartingPosition();
        //Pointorg.locationtech.geomesa.process.analytic.Point2PointProcess()

        Geometry g1 = (Geometry) granule1.getDefaultGeometry();
        Geometry g2 = (Geometry) granule2.getDefaultGeometry();

        Point gc1 = g1.getCentroid();
        Point gc2 = g2.getCentroid();
        logger.info(g1.getCentroid().toString());


        Double distance = gc1.distance(gc2);

        return distance;
    }

    private Double calculateIntersectRatio(SimpleFeature granule1, SimpleFeature granule2) {
        Geometry g1 = (Geometry) granule1.getDefaultGeometry();
        Geometry g2 = (Geometry) granule2.getDefaultGeometry();
        double areaRatio = 0.0;
        try {
            Geometry intersect = g1.intersection(g2);
            areaRatio = intersect.getArea() / g2.getArea();
            //logger.info((100 * areaRatio) + " %");
        } catch (Throwable e) {
            logger.error(e.getMessage());
            areaRatio = 0.0;
        }
        return areaRatio;
    }


    private Double calculateFinalValue(HashMap<String, Double> valueSet, Boolean isAnAggregate,
                                       String aggregationMethod, HashMap<String, Double> customAttributes) {
        Double finalValue;

        //if an aggregation process. there are two types, aggregation vs interpolation.
        if (!isAnAggregate) {
            switch (aggregationMethod) {
                case AggregationUtil.MEAN:
                    finalValue = AggregateInvoker.mean(valueSet);
                    break;
                case AggregationUtil.SUM:
                    finalValue = AggregateInvoker.sum(valueSet);
                    break;
                case AggregationUtil.MAX:
                    finalValue = AggregateInvoker.max(valueSet);
                    break;
                case AggregationUtil.MIN:
                    finalValue = AggregateInvoker.min(valueSet);
                    break;
                case AggregationUtil.INVERSE_DISTANCE:
                    finalValue = AggregateInvoker.inverseDistance(valueSet, customAttributes);
                    break;
                case AggregationUtil.AREA_BASED_AVERAGE:
                    finalValue = AggregateInvoker.areaBasedAverage(valueSet, customAttributes);
                    break;
                case "None":
                    finalValue = AggregateInvoker.defaultAggregate(valueSet);
                    break;
                default:
                    finalValue = -0.4;
                    break;
            }
        } else {
            finalValue = -0.002;
        }
        return finalValue;
    }

    public SimpleFeatureCollection getFeatures(String featureTypeName) throws IOException {
        SimpleFeatureCollection features = this.getFeaturesFromDatastore(featureTypeName);

//        SimpleFeatureType featureType = this.getFeatureType(featureTypeName);
//        String spatialGranularity = featureType.getUserData().get("spatialGranularity").toString();
//        String temporalGranularity = featureType.getUserData().get("temporalGranularity").toString();
//
//        SpatioTemporalFeatureType featureCollection =
//                new SpatioTemporalFeatureType(featureType, features, temporalGranularity, spatialGranularity);
//        return  featureCollection;

        return features;
    }

    //get features from the datastore
    public SimpleFeatureCollection getFeaturesFromDatastore(String typeName) throws IOException {
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
                                                            String uuid, String distinctID) {
        ArrayList<SimpleFeature> featureList = new ArrayList<SimpleFeature>();
        try {
//            Filter filterr = CQL.toFilter(
//                    "id = " + distinctID +
//                            "' AND dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");
//            Query queryyy = new Query(typeName,filterr);
//
//            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
//                    this.dataStore.getFeatureReader(queryyy, Transaction.AUTO_COMMIT);
//
//            while (reader.hasNext()) {
//                SimpleFeature feature = (SimpleFeature) reader.next();
//                int a = 2;
//            }

//            Filter filter = CQL.toFilter(
//                    indexCol + "='" + distinctID +
//                            "' AND dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");

//            Filter filter = CQL.toFilter(
//                    "id='" + distinctID +
//                            "' AND dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");

//            FilterFactory ff = CommonFactoryFinder.getFilterFactory();
//            Set<FeatureId> selection = new HashSet<>();
//            selection.add(ff.featureId(distinctID));
////            Filter filterS = ff.id(selection);
//
            String distinctIDLower = distinctID;
            distinctIDLower = distinctIDLower.toLowerCase(Locale.getDefault());

            Filter filter = ECQL.toFilter(
                    uuid + " ILIKE '" + distinctIDLower +
                            "' AND dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");

//            Filter filter = CQL.toFilter("dtg DURING " + startingDate + ":00.000/" + endDate + ":00.000");
            Query query = new Query(typeName, filter);
            //query.getHints().put(QueryHints.EXACT_COUNT(), Boolean.TRUE);

            FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                    this.dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            while (reader.hasNext()) {
                SimpleFeature feature = (SimpleFeature) reader.next();
                featureList.add(feature);
            }
            reader.close();
        } catch (Throwable e) {
            logger.error(e.getMessage());
        }
        return featureList;
    }

    public SimpleFeatureType getFeatureType(String featureTypeName) {
        try {
            SimpleFeatureType featureType = this.dataStore.getSchema(featureTypeName);
            return featureType;
        } catch (Exception e) {
            return null;
        }
    }

    public String getFeatureIndexColName(String featureTypeName) {
        String indexCol = "";
        try {
            List attributeTypes = this.dataStore.getSchema(featureTypeName).getTypes();
            AttributeTypeImpl attr = (AttributeTypeImpl) attributeTypes.get(0);
            indexCol = attr.getName().toString();
            String a = "";
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return indexCol;
    }
}
