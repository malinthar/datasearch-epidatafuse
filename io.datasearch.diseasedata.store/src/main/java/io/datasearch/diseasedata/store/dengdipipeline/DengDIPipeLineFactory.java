package io.datasearch.diseasedata.store.dengdipipeline;

import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.AggregationConfig;
import io.datasearch.diseasedata.store.dengdipipeline.models.configmodels.GranularityRelationConfig;
import io.datasearch.diseasedata.store.schema.SchemaBuilder;
import io.datasearch.diseasedata.store.schema.SimpleFeatureTypeSchema;
import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import io.datasearch.diseasedata.store.util.DataStoreLoader;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;


/**
 * Factory for creating pipelines.
 */
public class DengDIPipeLineFactory {

    private static final Logger logger = LoggerFactory.getLogger(DengDIPipeLineFactory.class);
    private static final String ZOOKEEPERS_KEY = "hbase.zookeepers";
    private static final String CATALOG_KEY = "hbase.catalog";

    public static DengDIPipeLine createDengDIPipeLine() {
        try {
            Map<String, Object> schemaConfigurations =
                    ConfigurationLoader.getSchemaConfigurations();
            String[] args = {"--".concat(ZOOKEEPERS_KEY),
                    (String) schemaConfigurations.get(ZOOKEEPERS_KEY),
                    "--".concat(CATALOG_KEY),
                    (String) schemaConfigurations.get(CATALOG_KEY)};
            DataStore dataStore = DataStoreLoader.findDataStore(args);
            Map<String, SimpleFeatureTypeSchema> schemas =
                    SchemaBuilder.buildSchema(schemaConfigurations
                            , dataStore);

            HashMap<String, GranularityRelationConfig> granularityConfigs = buildGranularityRelationConfigs();
            HashMap<String, AggregationConfig> aggregationConfigs = buildAggregationConfigs();

            return new DengDIPipeLine(dataStore, schemas, granularityConfigs, aggregationConfigs);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error building schema", e);
        }
    }

    public static HashMap<String, GranularityRelationConfig> buildGranularityRelationConfigs() {
        //read from the files and return a config file for each feature:tobe implemented

        HashMap<String, GranularityRelationConfig> granularityConfigs =
                new HashMap<String, GranularityRelationConfig>();

        String featureTypeName = "precipitation";
        GranularityRelationConfig config = new GranularityRelationConfig(featureTypeName,
                "weatherstations", "nearest", "moh", "week");

        config.setCustomAttributes("neighbors", "3");
        config.setCustomAttributes("maxDistance", "100000");

        granularityConfigs.put(featureTypeName, config);

        return granularityConfigs;
    }

    public static HashMap<String, AggregationConfig> buildAggregationConfigs() {
        HashMap<String, AggregationConfig> aggregationConfigs = new HashMap<String, AggregationConfig>();

        String featureTypeName = "precipitation";
        HashMap<String, String> customAttr = new HashMap<String, String>();
        AggregationConfig config =
                new AggregationConfig(featureTypeName, "StationName", "aggregation",
                        "mean", "ObservedValue", customAttr);

        aggregationConfigs.put(featureTypeName, config);
        return aggregationConfigs;
    }
}
