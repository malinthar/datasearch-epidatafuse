package io.datasearch.diseasedata.store.dengdipipeline;

import io.datasearch.diseasedata.store.dengdipipeline.fuseengine.FuseEngine;
import io.datasearch.diseasedata.store.dengdipipeline.fuseengine.GranularityRelationMapper;
import io.datasearch.diseasedata.store.dengdipipeline.ingestion.DataIngester;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.GranularityRelationConfig;
import io.datasearch.diseasedata.store.dengdipipeline.publish.Publisher;
import io.datasearch.diseasedata.store.dengdipipeline.stream.StreamHandler;
import io.datasearch.diseasedata.store.schema.SimpleFeatureTypeSchema;

import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * DengDIPipeline is the processing unit for streaming and historical data.
 * PipeLine starts with streaming component which retrieves streaming data and ends with data publisher which publishes
 * aggregated data into specified endpoints.
 */
public class DengDIPipeLine {
    private static final Logger logger = LoggerFactory.getLogger(DengDIPipeLine.class);
    //Data store for persisting spatio-temporal data.
    private DataStore dataStore;
    //Stream handler for handling data
    private Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas;
    private StreamHandler streamHandler;
    //aggregator and transformer;
    private FuseEngine fuseEngine;
    //publisher for publishing data to relevant endpoints
    private Publisher publisher;

    //private Map<String, GranularityMap> spatialGranularityMap;

    public DengDIPipeLine(DataStore dataStore, Map<String, SimpleFeatureTypeSchema> schemas) {
        this.dataStore = dataStore;
        this.simpleFeatureTypeSchemas = schemas;
        this.streamHandler = new StreamHandler();
        this.fuseEngine = new FuseEngine(this.dataStore);
    }

    public DataStore getDataStore() {
        return this.dataStore;
    }

    public SimpleFeatureTypeSchema getSchema(String featureTypeName) {
        return simpleFeatureTypeSchemas.get(featureTypeName);
    }

    public FuseEngine getFuseEngine() {
        if (this.fuseEngine == null) {
            this.fuseEngine = new FuseEngine(dataStore);
        }
        return this.fuseEngine;
    }

    public void ingest() {
        try {
            DataIngester dataIngester = new DataIngester();
            dataIngester.insertData(this.getDataStore(), this.simpleFeatureTypeSchemas);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void mapGranularityRelations() {
        logger.info("h00");
        GranularityRelationMapper granularityRelationMapper = this.fuseEngine.getGranularityRelationMapper();

        logger.info("h1");
        this.fuseEngine.setSpatialGranularityConfigs();
        this.fuseEngine.setTargetGranularities();
        logger.info("h2");

        HashMap<String, GranularityRelationConfig> relationConfigs = this.fuseEngine.getSpatialGranularityConfigs();

        relationConfigs.forEach((featureType, config) -> {
            logger.info(featureType + config);
            granularityRelationMapper.buildSpatialGranularityMap(config);
        });
    }

//    public void mapGranularityRelations() {
//        try {
//            GranularityRelationMapper grmapper = new GranularityRelationMapper(this.dataStore);
//            grmapper.buildGranularityMap();
//            Map<String, GranularityMap> spatialGranularityMap = grmapper.getSpatialGranularityMap();
//
//            this.spatialGranularityMap = spatialGranularityMap;
//
//            spatialGranularityMap.forEach((feature, map) -> {
//                logger.info(feature + " mapper " + map.getClass().getSimpleName());
//            });
//
//        } catch (Exception e) {
//            logger.error(e.getMessage());
//        }
//    }
//
//    public void convertIntoRequiredGranule(String featureType) {
//
//        String granulityType = "weatherstations";
//        String spatialMappingMethod = this.spatialGranularityMap.get(granulityType).getClass().getSimpleName();
//        String temporalMappingMethod = "";
//
//        logger.info(spatialMappingMethod);
//
//        GranularityConvertor granularityConvertor =
//                new GranularityConvertor(this.dataStore, this.spatialGranularityMap);
//
//        try {
//            granularityConvertor.convert(featureType, spatialMappingMethod, temporalMappingMethod);
//        } catch (Exception e) {
//            logger.info(e.getMessage());
//        }
//
//    }
}
