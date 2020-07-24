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

    public static DengDIPipeLine createDengDIPipeLine(String[] args) {
        try {
            DataStore dataStore = DataStoreLoader.findDataStore(args);
            Map<String, SimpleFeatureTypeSchema> schemas =
                    SchemaBuilder.buildSchema(ConfigurationLoader.getSchemaConfigurations(), dataStore);
            return new DengDIPipeLine(dataStore, schemas);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null; //todo:don't return null
    }
}
