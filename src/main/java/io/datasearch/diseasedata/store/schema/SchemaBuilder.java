package io.datasearch.diseasedata.store.schema;

import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SchemaBuilder class for creating new feature types.
 */
public class SchemaBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SchemaBuilder.class);
    private static final String SCHEMA_CONFIGURATIONS_KEY = "simple_feature_types";

    public static void buildSchema(Map<String, Object> schemaConfigurations, DataStore dataStore) {
        try {
            List<Map<String, Object>> simpleFeatureTypeConfigs =
                    (ArrayList) schemaConfigurations.get(SCHEMA_CONFIGURATIONS_KEY);
            for (Map<String, Object> simpleFeatureTypeConfig : simpleFeatureTypeConfigs) {
                SimpleFeatureTypeSchema simpleFeatureTypeSchema = new SimpleFeatureTypeSchema(simpleFeatureTypeConfig);
                SimpleFeatureType simpleFeatureType = simpleFeatureTypeSchema.getSimpleFeatureType();
                dataStore.createSchema(simpleFeatureType);
                ensureSchema(dataStore, simpleFeatureTypeSchema.getSimpleFeatureType().getTypeName());
                logger.info("Creating schema: " + DataUtilities.encodeType(simpleFeatureType));

            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error creating schema:", e);
        }
        logger.info("Done! DataStore is created");
    }

    public static void ensureSchema(DataStore datastore, String simpleFeatureTypeName) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(simpleFeatureTypeName);
        if (sft == null) {
            throw new IllegalStateException("Schema '" + simpleFeatureTypeName + "' does not exist.");
        }
    }
}
