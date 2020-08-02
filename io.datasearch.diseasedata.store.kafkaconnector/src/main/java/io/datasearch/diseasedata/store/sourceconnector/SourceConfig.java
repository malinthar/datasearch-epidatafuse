package io.datasearch.diseasedata.store.sourceconnector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

/**
 * Source config.
 */
public class SourceConfig extends AbstractConfig {

    //source url
    public static final String BASE_URL_CONFIG = "diseasedata.base.url";
    private static final String BASE_URL_DEFAULT = "http://localhost:5000";
    private static final String BASE_URL_DOC = "URL to Source API";

    //kafka topic to produce to
    public static final String TOPIC_CONFIG = "diseasedata.topic";
    private static final String TOPIC_DEFAULT = "diseasedata";
    private static final String TOPIC_DOC = "Topic to publish disease data to.";

    public static final String POLL_INTERVAL = "poll.interval";
    private static final int POLL_INTERVAL_DEFAULT = 100000;
    private static final String POLL_INTERVAL_DOC = "Poll interval in milliseconds.";


    /**
     * Two simple configs. Source url and target kafka topic.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BASE_URL_CONFIG, ConfigDef.Type.STRING,
                    BASE_URL_DEFAULT,
                    new NonEmptyStringWithoutControlChars(),
                    ConfigDef.Importance.HIGH, BASE_URL_DOC)
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING,
                    TOPIC_DEFAULT, new NonEmptyStringWithoutControlChars(),
                    ConfigDef.Importance.HIGH, TOPIC_DOC)
            .define(POLL_INTERVAL, ConfigDef.Type.INT,
                    POLL_INTERVAL_DEFAULT, ConfigDef.Importance.LOW,
                    POLL_INTERVAL_DOC);


    public SourceConfig(Map<String, ?> props) {
        super(CONFIG_DEF, props);
    }

    /**
     * Assign default values if some value is missing.
     *
     * @return
     */
    Map<String, String> returnPropertiesWithDefaultsValuesIfMissing() {
        Map<String, ?> uncastProperties = this.values();
        Map<String, String> config = new HashMap<>(uncastProperties.size());
        uncastProperties.forEach((key, valueToBeCast) -> config.put(key, valueToBeCast.toString()));
        return config;
    }

    static final class NonEmptyStringWithoutControlChars extends ConfigDef.NonEmptyStringWithoutControlChars {
        //Only here to create nice human readable for exporting to documentation.
        @Override
        public String toString() {
            return "non-empty string and no ISO control characters";
        }
    }


}
