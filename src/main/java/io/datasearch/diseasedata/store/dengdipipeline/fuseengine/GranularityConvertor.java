package io.datasearch.diseasedata.store.dengdipipeline.fuseengine;

import io.datasearch.diseasedata.store.util.ConfigurationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * For granularity conversion.
 */
public class GranularityConvertor {
    private static final Logger logger = LoggerFactory.getLogger(GranularityConvertor.class);

    public List<Map<String, Object>> loadFeatureGranularities() {
        try {
            Map<String, Object> granularityConfigurations = ConfigurationLoader.getGranularityConfigurations();

            List<Map<String, Object>> featuregranularities =
                    (ArrayList) granularityConfigurations.get("feature_granularities");

            //for (Map<String, Object> featuregranularity : featuregranularities) {
            //logger.info("Reading Granularities: " + featuregranularity.toString() + "\n");
            //}

            return featuregranularities;

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error reading granularity config:", e);
        }
    }

    public Map<String, Object> loadAggrigationGranularities() {
        try {
            Map<String, Object> granularityConfigurations = ConfigurationLoader.getGranularityConfigurations();

            Map<String, Object> aggrigategranularities =
                    (Map<String, Object>) granularityConfigurations.get("aggregation_granularities");

            //for (Map<String, Object> featuregranularity : featuregranularities) {
            //logger.info("Reading Granularities: " + featuregranularity.toString() + "\n");
            //}

            return aggrigategranularities;

        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new RuntimeException("Error reading granularity config:", e);
        }
    }

    public void temporalConversion() {
    }

    public void spatialConversion() {
    }
}
