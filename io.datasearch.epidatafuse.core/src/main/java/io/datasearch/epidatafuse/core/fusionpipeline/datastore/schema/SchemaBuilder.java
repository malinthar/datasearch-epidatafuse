package io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema;

import io.datasearch.epidatafuse.core.util.FeatureConfig;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SchemaBuilder class for creating new feature types.
 */
public class SchemaBuilder {
    private static final Logger logger = LoggerFactory.getLogger(SchemaBuilder.class);

    public static Map<String, SimpleFeatureTypeSchema> buildSchemas(
            List<FeatureConfig> featureConfigs, DataStore dataStore) {

        Map<String, SimpleFeatureTypeSchema> schemas = new HashMap<>();
        try {
            for (FeatureConfig featureConfig : featureConfigs) {
                SimpleFeatureTypeSchema schema = buildSchema(featureConfig, dataStore);
                schemas.put(schema.getSimpleFeatureType().getTypeName(), schema);
            }
            logger.info("DataStore is created successfully!");
            return schemas;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error building schema", e);
        }
    }

    public static SimpleFeatureTypeSchema buildSchema(FeatureConfig featureConfig,
                                                      DataStore dataStore) {
        try {
            SimpleFeatureTypeSchema simpleFeatureTypeSchema =
                    new SimpleFeatureTypeSchema(featureConfig);
            SimpleFeatureType simpleFeatureType = simpleFeatureTypeSchema.getSimpleFeatureType();
            dataStore.createSchema(simpleFeatureType);
            ensureSchema(dataStore, simpleFeatureTypeSchema.getSimpleFeatureType().getTypeName());
            logger.info("Created schema: " + DataUtilities.encodeType(simpleFeatureType));
            return simpleFeatureTypeSchema;
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error building schema", e);
        }
    }

    public static void ensureSchema(DataStore datastore,
                                    String simpleFeatureTypeName) throws IOException {
        SimpleFeatureType sft = datastore.getSchema(simpleFeatureTypeName);
        if (sft == null) {
            throw new IllegalStateException("Schema '" + simpleFeatureTypeName + "' does not exist.");
        }
    }
}
