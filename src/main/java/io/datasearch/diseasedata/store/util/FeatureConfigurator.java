package io.datasearch.diseasedata.store.util;

import io.datasearch.diseasedata.store.DiseaseDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.InputStream;
import java.util.Map;

/**
 * Feature configurator.
 */
public class FeatureConfigurator {
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    public static Map<String, Object> getFeatureConfiguration() {
        Map<String, Object> configuration = null;
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = FeatureConfigurator.class
                    .getClassLoader()
                    .getResourceAsStream("config.yaml");
            configuration = yaml.load(inputStream);
        } catch (YAMLException e) {
            logger.error(e.getMessage());
        }
        return configuration;
    }
}
