package io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema;

import java.util.Map;

/**
 * Value transformations.
 */
public class AttributeTransformation {
    private String attributeName;
    private String transformation;
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";
    private static final String TRANSFORMATION_KEY = "transformation";

    public AttributeTransformation(Map<String, String> transformation) {
        this.attributeName = transformation.get(ATTRIBUTE_NAME_KEY);
        this.transformation = transformation.get(TRANSFORMATION_KEY);
    }

    public String getAttributeName() {
        return attributeName;
    }

    public String getTransformation() {
        return transformation;
    }
}
