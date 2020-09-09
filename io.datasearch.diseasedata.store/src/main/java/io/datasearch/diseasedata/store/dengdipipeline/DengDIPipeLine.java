package io.datasearch.diseasedata.store.dengdipipeline;

import io.datasearch.diseasedata.store.dengdipipeline.fuseengine.FuseEngine;
import io.datasearch.diseasedata.store.dengdipipeline.ingestion.DataIngester;
import io.datasearch.diseasedata.store.dengdipipeline.models.granularityrelationmap.GranularityMap;
import io.datasearch.diseasedata.store.dengdipipeline.models.granularityrelationmap.SpatialGranularityRelationMap;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.AggregationConfig;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.GranularityRelationConfig;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.IngestConfig;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.SchemaConfig;
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

    //store configurations reads from the user inputs
    private HashMap<String, GranularityRelationConfig> granularityRelationConfigs;
    private HashMap<String, AggregationConfig> aggregationConfigs;
    private HashMap<String, IngestConfig> ingestConfigs;
    private HashMap<String, SchemaConfig> schemaConfigs;

    //this stores granularity maps for each feature build according to the configs
    private HashMap<String, GranularityMap> granularityRelationMaps;

    public DengDIPipeLine(DataStore dataStore, Map<String, SimpleFeatureTypeSchema> schemas,
                          HashMap<String, GranularityRelationConfig> granularityRelationConfigs) {
        this.dataStore = dataStore;
        this.simpleFeatureTypeSchemas = schemas;
        this.streamHandler = new StreamHandler();
        this.fuseEngine = new FuseEngine(this.dataStore);
        this.granularityRelationConfigs = granularityRelationConfigs;
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
        this.granularityRelationMaps = this.fuseEngine.buildGranularityMap(granularityRelationConfigs);
    }
}
