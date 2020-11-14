package io.datasearch.epidatafuse.core.fusionpipeline;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.PipelineDataStore;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.fusionpipeline.fuseengine.FuseEngine;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.AggregationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.GranularityRelationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularityrelationmap.GranularityMap;
import io.datasearch.epidatafuse.core.fusionpipeline.publish.Publisher;
import io.datasearch.epidatafuse.core.fusionpipeline.stream.StreamHandler;
import io.datasearch.epidatafuse.core.util.FeatureConfig;
import io.datasearch.epidatafuse.core.util.IngestConfig;
import io.datasearch.epidatafuse.core.util.IngestionConfig;
import io.datasearch.epidatafuse.core.util.PipelineInfo;
import io.datasearch.epidatafuse.core.util.SchemaConfig;
import io.siddhi.core.event.Event;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * FusionPipeline processes streaming and historical data to generate fused data points.
 * PipeLine starts with streaming component which retrieves streaming data and ends with data publisher which publishes
 * aggregated data into specified endpoints.
 */
public class FusionPipeline {
    private static final Logger logger = LoggerFactory.getLogger(FusionPipeline.class);
    private PipelineDataStore pipelineDataStore;
    private StreamHandler streamHandler;
    private FuseEngine fuseEngine;
    private Publisher publisher;
    private Map<String, GranularityRelationConfig> granularityRelationConfigs;
    private Map<String, AggregationConfig> aggregationConfigs;
    private Map<String, IngestConfig> ingestConfigs;
    private Map<String, SchemaConfig> schemaConfigs;
    private Map<String, GranularityMap> granularityRelationMaps;
    private String pipelineName;

    public FusionPipeline(String pipelineName, DataStore dataStore,
                          Map<String, SimpleFeatureTypeSchema> featureSFTSchemas,
                          Map<String, SimpleFeatureTypeSchema> granularitySFTSchemas,
                          Map<String, GranularityRelationConfig> granularityRelationConfigs,
                          Map<String, AggregationConfig> aggregationConfigs) {

        this.pipelineName = pipelineName;
        this.pipelineDataStore = new PipelineDataStore(dataStore, featureSFTSchemas, granularitySFTSchemas);
        this.streamHandler = new StreamHandler(this);
        this.fuseEngine = new FuseEngine(dataStore, granularityRelationConfigs, aggregationConfigs);
        this.granularityRelationConfigs = granularityRelationConfigs;
        this.aggregationConfigs = aggregationConfigs;
    }

    public void init() {
        this.streamHandler.init();
    }

    public void terminate() {
        this.streamHandler.terminateSourceConnections();
    }

    public void addFeature(
            SimpleFeatureTypeSchema schema, GranularityRelationConfig granularityRelationConfig) {
        this.pipelineDataStore.addFeatureSchema(schema);
        this.granularityRelationConfigs.put(schema.getSimpleFeatureTypeName(), granularityRelationConfig);
    }

    public void addGranularity(SimpleFeatureTypeSchema schema) {
        this.pipelineDataStore.addGranularitySchema(schema);
    }

    public DataStore getDataStore() {
        return this.pipelineDataStore.getDataStore();
    }

    public SimpleFeatureTypeSchema getSchema(String featureTypeName) {
        return this.pipelineDataStore.getSchema(featureTypeName);
    }

    public FuseEngine getFuseEngine() {
        return this.fuseEngine;
    }

    public void bulkIngest(IngestionConfig ingestionConfig) {
        this.pipelineDataStore.bulkIngest(ingestionConfig);
    }

    public Boolean bulkIngest(IngestConfig ingestConfig, String featureType) {
        if (FeatureConfig.VARIABLE_TYPE_IDENTIFIER.equals(featureType)) {
            return this.pipelineDataStore.bulkIngest(ingestConfig, featureType);
        } else if (FeatureConfig.GRANULARITY_TYPE_IDENTIFIER.equals(featureType)) {
            return this.pipelineDataStore.ingestGranules(ingestConfig);
        } else {
            return false;
        }
    }

    public void mapGranularityRelations() {
        this.granularityRelationMaps = this.fuseEngine.buildGranularityMap(granularityRelationConfigs);
    }

    public void aggregate() throws IOException {
        String featureType = "precipitation";
        this.fuseEngine
                .aggregate(granularityRelationMaps.get(featureType), this.aggregationConfigs.get(featureType));
    }

    public void streamingIngest(Event[] events, String featureType) {
        pipelineDataStore.streamingIngest(events, featureType);
    }

    public PipelineInfo getInfo() {
        return new PipelineInfo(this.pipelineName, this.pipelineDataStore.getSchemas(),
                this.pipelineDataStore.getGranularitySchemas(), granularityRelationConfigs, aggregationConfigs);
    }
}
