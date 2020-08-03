package io.datasearch.diseasedata.store.sourceconnector.deserializer;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

/**
 * StringDeserializer to deserialize received records.
 */
public class StringDeserializer implements JsonDeserializer<String> {
    private static final Logger logger = LoggerFactory.getLogger(StringDeserializer.class);

    @Override
    public String deserialize(JsonElement jsonElement, Type type,
                              JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
        if (jsonElement.isJsonNull() || jsonElement.getAsString().isEmpty()) {
            throw new JsonParseException("Unable to deserialize [" + jsonElement + "] to a String.");
        }
        logger.info("cjdnvjfnjfvnjfvnjfvn" + jsonElement.toString());
        String json = jsonElement.toString();
        return json;
    }
}

