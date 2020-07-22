package io.datasearch.diseasedata.store.schema;

import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.List;
import java.util.Map;

/**
 * Schema for attributes.
 */
public class SimpleFeatureTypeSchema implements DiseaseDataSchema {
    private SimpleFeatureType sft = null;
    private String typeName;

    public SimpleFeatureTypeSchema(Map<String, Object> parameters) {
        this.typeName = (String) parameters.get("feature_name");
        buildSimpleFeature((List<Map<String, String>>) parameters.get("attributes"));
    }

    @Override
    public String getTypeName() {
        return this.typeName;
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        return sft;
    }

    public void buildSimpleFeature(List<Map<String, String>> attributes) {
        if (sft == null) {
            StringBuilder featureAttributes = new StringBuilder();
            for (int i = 0; i < attributes.size(); i++) {
                featureAttributes.append(attributes.get(i).get("attribute"));
                if (i != attributes.size() - 1) {
                    featureAttributes.append(",");
                }
            }
            sft = SimpleFeatureTypes.createType(getTypeName(), featureAttributes.toString());
        }
    }
}
