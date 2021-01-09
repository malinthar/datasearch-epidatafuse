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

import java.util.Date;
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
    private String initTimestamp;
    private String initialTimestamp;
    private static final String URL_KEY = "url";

    public FusionPipeline(String pipelineName, DataStore dataStore,
                          Map<String, SimpleFeatureTypeSchema> featureSFTSchemas,
                          Map<String, SimpleFeatureTypeSchema> granularitySFTSchemas,
                          Map<String, GranularityRelationConfig> granularityRelationConfigs,
                          Map<String, AggregationConfig> aggregationConfigs) {

        this.pipelineName = pipelineName;
        this.pipelineDataStore = new PipelineDataStore(dataStore, featureSFTSchemas, granularitySFTSchemas);
        this.streamHandler = new StreamHandler(this);
        this.fuseEngine =
                new FuseEngine(pipelineDataStore, pipelineName, granularityRelationConfigs, aggregationConfigs);
        this.granularityRelationConfigs = granularityRelationConfigs;
        this.aggregationConfigs = aggregationConfigs;
    }

    public void setFusionFrequency(long fusionFrequency) {
        this.fuseEngine.setFusionFrequency(fusionFrequency);
    }

    public void setFusionFQUnit(String fusionFQUnit) {
        this.fuseEngine.setFusionFQUnit(fusionFQUnit);
    }

    public void setFusionFQMultiplier(String fusionFQMultiplier) {
        this.fuseEngine.setFusionFQMultiplier(fusionFQMultiplier);
    }

    public void init(String initialTimestamp) {
        this.initialTimestamp = initialTimestamp;
        this.streamHandler.init();
        this.mapGranularityRelations();
        this.invokeAggregate();
        this.initTimestamp = new Date().toString();
    }

    public void terminate() {
        this.streamHandler.terminateSourceConnections();
    }

    public void addFeature(
            SimpleFeatureTypeSchema schema, GranularityRelationConfig granularityRelationConfig,
            AggregationConfig aggregationConfig) {
        this.pipelineDataStore.addFeatureSchema(schema);
        this.granularityRelationConfigs.put(schema.getSimpleFeatureTypeName(), granularityRelationConfig);
        this.aggregationConfigs.put(schema.getSimpleFeatureTypeName(), aggregationConfig);
    }

    public void addGranularity(SimpleFeatureTypeSchema schema) {
        this.pipelineDataStore.addGranularitySchema(schema);
    }

    public void addStreamingConfig(String featureName, Map<String, Object> parameters) {
        this.getSchema(featureName).setExternalSourceAPIURL((String) parameters.get(URL_KEY));
        this.streamHandler.addStreamingConfiguration(featureName, parameters);
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
        this.granularityRelationMaps = this.fuseEngine.invokeGranularityMappingProcess(granularityRelationConfigs);
    }

//    public void aggregate() throws IOException {
//        String dtg = LocalDateTime.now().toString();
//
//        DataFrame dataFrame = new DataFrame(dtg);
//
//        if (this.granularityRelationMaps.size() != 0) {
//            granularityRelationMaps.forEach((String featureType, GranularityMap map) -> {
//                try {
//                    SpatioTemporallyAggregatedCollection spatioTemporallyAggregatedCollection =
//                            this.fuseEngine.aggregate(map, this.aggregationConfigs.get(featureType));
//                    dataFrame.addAggregatedFeatureType(spatioTemporallyAggregatedCollection);
//                } catch (Exception e) {
//                    e.getMessage();
//                }
//            });
//
//        }
//
//        ArrayList<String> csvRecords = dataFrame.createCSVRecords();
//        int a = 1;
//
//    }

    public void invokeAggregate() {
        this.fuseEngine.scheduleTasks();
    }

    public void streamingIngest(Event[] events, String featureType) {
        pipelineDataStore.streamingIngest(events, featureType);
    }

    public PipelineInfo getInfo() {
        return new PipelineInfo(this.pipelineName, this.pipelineDataStore.getSchemas(),
                this.pipelineDataStore.getGranularitySchemas(),
                this.granularityRelationConfigs, this.aggregationConfigs,
                this.fuseEngine.getFusionFrequency(), this.fuseEngine.getFusionFQUnit(),
                this.fuseEngine.getFusionFQMultiplier(),
                this.initTimestamp, this.initialTimestamp);
    }
}
