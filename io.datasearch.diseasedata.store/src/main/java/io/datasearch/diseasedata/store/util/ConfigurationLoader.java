package io.datasearch.diseasedata.store.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.InputStream;
import java.util.Map;

/**
 * Feature configurator.
 */
public class ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);

    private static final String SCHEMA_CONFIG_DIR = "config-schema.yaml";
    private static final String INGESTION_HISTORICAL_CONFIG_DIR = "config-ingest-historical.yaml";
    private static final String QUERY_CONFIG_DIR = "config-queries.yaml";
    private static final String GRANULARITY_CONFIG_DIR = "config-granularity.yaml";
    private static final String GRANULARITY_MAPPING_CONFIG_DIR = "config-granularity-mapping.yaml";
    private static final String INGESTION_STREAMING_CONFIG_DIR = "config-ingest-streaming.yaml";


    public static Map<String, Object> getSchemaConfigurations() {
        return getConfigurations(SCHEMA_CONFIG_DIR);
    }

    public static Map<String, Object> getIngestConfigurations() {
        return getConfigurations(INGESTION_HISTORICAL_CONFIG_DIR);
    }

    public static Map<String, Object> getQueryConfigurations() {
        return getConfigurations(QUERY_CONFIG_DIR);
    }

    public static Map<String, Object> getGranularityConfigurations() {
        return getConfigurations(GRANULARITY_CONFIG_DIR);
    }

    public static Map<String, Object> getGranularityMappingConfigurations() {
        return getConfigurations(GRANULARITY_MAPPING_CONFIG_DIR);
    }

    public static Map<String, Object> getIngestStreamingConfigurations() {
        return getConfigurations(INGESTION_STREAMING_CONFIG_DIR);
    }

    public static Map<String, Object> getConfigurations(String configFileDir) {
        Map<String, Object> configuration = null;
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = ConfigurationLoader.class
                    .getClassLoader()
                    .getResourceAsStream(configFileDir);
            configuration = yaml.load(inputStream);
        } catch (YAMLException e) {
            logger.error(e.getMessage());
        }
        return configuration;
    }
}
