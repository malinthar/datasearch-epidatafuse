package io.datasearch.diseasedata.store.dengdipipeline;

import io.datasearch.diseasedata.store.schema.SchemaBuilder;
import io.datasearch.diseasedata.store.schema.SimpleFeatureTypeSchema;
import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import io.datasearch.diseasedata.store.util.DataStoreLoader;
import org.geotools.data.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            return new DengDIPipeLine(dataStore, schemas);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error building schema", e);
        }
    }
}
