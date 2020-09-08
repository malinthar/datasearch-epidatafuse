package io.datasearch.diseasedata.store.dengdipipeline.datastore.schema;

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
    private List<Map<String, String>> attributes;
    private Map<String, String> configurations;


    public SimpleFeatureTypeSchema(Map<String, Object> parameters) {
        this.typeName = (String) parameters.get("feature_name");
        this.attributes = (List<Map<String, String>>) parameters.get("attributes");
        this.configurations = (Map<String, String>) parameters.get("configurations");
        buildSimpleFeature(this.attributes);
    }

    @Override
    public String getTypeName() {
        return this.typeName;
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        return sft;
    }

    public List<Map<String, String>> getAttributes() {
        return this.attributes;
    }

    public Map<String, String> getConfigurations() {
        return this.configurations;
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
