package io.datasearch.epidatafuse.core.fusionpipeline;

import io.datasearch.epidatafuse.core.ServerContext;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.ingestion.util.DataStoreLoader;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SchemaBuilder;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.AggregationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.configuration.GranularityRelationConfig;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod.TemporalRelationship;
import io.datasearch.epidatafuse.core.fusionpipeline.util.PipelineUtil;
import io.datasearch.epidatafuse.core.util.FeatureConfig;
import io.datasearch.epidatafuse.core.util.IngestConfig;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Pipeline controller.
 */
public class FusionPipeLineController {

    private static final Logger logger = LoggerFactory.getLogger(FusionPipeLineController.class);

    public static void createFusionPipeLine(String pipelineName) {
        try {
            DataStore dataStore = createDataStore(pipelineName + "10");
            Map<String, SimpleFeatureTypeSchema> featureSFTSchemas = new HashMap<>();
            Map<String, SimpleFeatureTypeSchema> granularitySFTSchemas = new HashMap<>();
            Map<String, GranularityRelationConfig> granularityRelationConfigs = new HashMap<>();
            Map<String, AggregationConfig> aggregationConfigs = new HashMap<>();
            FusionPipeline pipeline = new FusionPipeline(pipelineName, dataStore, featureSFTSchemas,
                    granularitySFTSchemas, granularityRelationConfigs, aggregationConfigs);
            ServerContext.addPipeline(pipelineName, pipeline);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error building schema", e);
        }
    }

    public static Boolean addNewFeature(String pipelineName, FeatureConfig featureConfig) {
        FusionPipeline pipeline = ServerContext.getPipeline(pipelineName);
        if (pipeline != null) {
            DataStore dataStore = pipeline.getDataStore();
            SimpleFeatureTypeSchema schema =
                    SchemaBuilder.buildSchema(featureConfig, dataStore);
            if (featureConfig.VARIABLE_TYPE_IDENTIFIER.equals(featureConfig.getFeatureType())) {
                GranularityRelationConfig granularityConfig =
                        new GranularityRelationConfig(schema.getSimpleFeatureTypeName(),
                                featureConfig.getGranularityRelationConfig());
                AggregationConfig aggregationConfig = new AggregationConfig(schema.getSimpleFeatureTypeName(),
                        featureConfig.getAggregationConfig());
                pipeline.addFeature(schema, granularityConfig, aggregationConfig);
            } else if (featureConfig.GRANULARITY_TYPE_IDENTIFIER.equals(featureConfig.getFeatureType())) {
                pipeline.addGranularity(schema);
            }
            return true;
        }
        return false;
    }

    public static Boolean ingestToFeature(String pipelineName, IngestConfig ingestConfig) {
        FusionPipeline pipeline = ServerContext.getPipeline(pipelineName);
        if (pipeline != null) {
            return pipeline.bulkIngest(ingestConfig, FeatureConfig.VARIABLE_TYPE_IDENTIFIER);
        }
        return false;
    }

    public static Boolean ingestToGranularity(String pipelineName, IngestConfig ingestConfig) {
        FusionPipeline pipeline = ServerContext.getPipeline(pipelineName);
        if (pipeline != null) {
            return pipeline.bulkIngest(ingestConfig, FeatureConfig.GRANULARITY_TYPE_IDENTIFIER);
        }
        return false;
    }

    public static DataStore createDataStore(String catalogName) {
        String[] args = {PipelineUtil.ARG_SEPARATOR.concat(PipelineUtil.ZOOKEEPERS_KEY),
                PipelineUtil.ZOOKEEPER,
                PipelineUtil.ARG_SEPARATOR.concat(PipelineUtil.CATALOG_KEY), catalogName};
        DataStore dataStore = DataStoreLoader.findDataStore(args);
        return dataStore;
    }

    public static Boolean initPipeline(String pipelineName, String initialTimestamp) {
        FusionPipeline pipeline = ServerContext.getPipeline(pipelineName);
        if (ServerContext.getPipeline(pipelineName) != null) {
            pipeline.init(initialTimestamp);
            return true;
        }
        return false;
    }

    public static Boolean setFusionFrequency(String pipelineName, String granularity, String multiplier) {
        FusionPipeline pipeline = ServerContext.getPipeline(pipelineName);
        try {
            pipeline.setFusionFrequency(
                    TemporalRelationship.getGranularityToSeconds(granularity) *
                            Integer.parseInt(multiplier));
            pipeline.setFusionFQUnit(granularity);
            pipeline.setFusionFQMultiplier(multiplier);
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }

//    @Deprecated
//    public static GranularityRelationConfig buildGranularityRelationConfig(
//            String featureTypeName, Map<String, Object> granularityConfig) {
//        return new GranularityRelationConfig(featureTypeName, granularityConfig);
//    }

//    @Deprecated
//    public static FusionPipeline createFusionPipeLine(SchemaConfig schemaConfig) {
//        try {
//            DataStore dataStore = createDataStore(schemaConfig.getPipelineName());
//            Map<String, SimpleFeatureTypeSchema> featureSFTSchemas =
//                    SchemaBuilder.buildSchemas(schemaConfig.getSimpleFeatureSchemaConfigs(), dataStore);
//            Map<String, SimpleFeatureTypeSchema> granularitySFTSchemas =
//                    SchemaBuilder.buildSchemas(schemaConfig.getSpatialGranularitySchemaConfigs(), dataStore);
//            Map<String, GranularityRelationConfig> granularityRelationConfigs =
//                    buildGranularityRelationConfigs(schemaConfig.getGranularityRelationConfigs());
//            Map<String, AggregationConfig> aggregationConfigs = buildAggregationConfigs();
//            return new FusionPipeline(schemaConfig.getPipelineName(), dataStore, featureSFTSchemas,
//                    granularitySFTSchemas, granularityRelationConfigs, aggregationConfigs);
//        } catch (Exception e) {
//            logger.error(e.getMessage());
//            throw new RuntimeException("Error building schema", e);
//        }
//    }
//
//    @Deprecated
//    public static Map<String, GranularityRelationConfig> buildGranularityRelationConfigs(
//            Map<String, Map<String, Object>> granularityConfigs) {
//
//        Map<String, GranularityRelationConfig> granularityRelationConfigs = new HashMap<>();
//        granularityConfigs.forEach((key, value) -> {
//            GranularityRelationConfig config = buildGranularityRelationConfig(key, value);
//            //todo: develop a set of classes for mapping methods
//            if (config.getSpatialRelationMappingMethod() != null) {
//                config.setMappingAttribute("neighbors", "3");
//                config.setMappingAttribute("maxDistance", "100000");
//            }
//            granularityRelationConfigs.put(key, config);
//        });
//        return granularityRelationConfigs;
//    }
}
