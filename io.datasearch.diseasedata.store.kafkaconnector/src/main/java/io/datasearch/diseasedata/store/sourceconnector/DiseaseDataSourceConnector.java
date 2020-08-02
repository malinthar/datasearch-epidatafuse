package io.datasearch.diseasedata.store.sourceconnector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source connector.
 */
public class DiseaseDataSourceConnector extends SourceConnector {
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataSourceConnector.class);
    private Map<String, String> configProperties;

    @Override
    public void start(Map<String, String> props) {

        logger.info("Disease data source connector");
        try {
            configProperties = setupSourcePropertiesWithDefaultsIfMissing(props);
        } catch (ConfigException e) {
            throw new ConnectException(
                    "Couldn't start Disease data source connector due to configuration error", e);
        }
    }

    private Map<String, String> setupSourcePropertiesWithDefaultsIfMissing(
            Map<String, String> props) throws ConfigException {
        return new SourceConfig(props).returnPropertiesWithDefaultsValuesIfMissing();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DiseaseDataSourceTask.class;
    }

    /**
     * We have only one task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks != 1) {
            logger.info("Ignoring maxTasks as there can only be one in the standalone mode");
        }
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.putAll(configProperties);
        configs.add(taskConfig);
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SourceConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
