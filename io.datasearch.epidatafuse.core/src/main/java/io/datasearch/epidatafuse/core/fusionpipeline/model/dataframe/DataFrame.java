package io.datasearch.epidatafuse.core.fusionpipeline.model.dataframe;

import io.datasearch.epidatafuse.core.fusionpipeline.model.datamodel.SpatioTemporallyAggregatedCollection;
import org.opengis.feature.simple.SimpleFeature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Dataframe class
 */

public class DataFrame {

    private String dtg;
    private String spatialGranularity;
    private String temporalGranularity;
    private ArrayList<String> featureTypeNames = new ArrayList<String>();
    private HashMap<String, SpatioTemporallyAggregatedCollection> aggregatedFeatures =
            new HashMap<String, SpatioTemporallyAggregatedCollection>();
    private HashMap<String, String> aggregatedAttributeNames = new HashMap<String, String>();
    private ArrayList<String> finalSpatialGranules = new ArrayList<String>();

    public DataFrame(String dtg) {
        this.dtg = dtg;
    }

    public void addAggregatedFeatureType(SpatioTemporallyAggregatedCollection collection) {
        this.featureTypeNames.add(collection.getFeatureTypeName());
        this.aggregatedFeatures.put(collection.getFeatureTypeName(), collection);
        Set<String> granuleSet = collection.getFeatureHashMap().keySet();
        for (String granule : granuleSet) {
            if (!finalSpatialGranules.contains(granule)) {
                finalSpatialGranules.add(granule);
            }
        }

        aggregatedAttributeNames.put(collection.getFeatureTypeName(), collection.getAggregatedAttributeList().get(0));
        if (this.temporalGranularity == null) {
            this.temporalGranularity = collection.getTemporalGranularity();
        }
        if (this.spatialGranularity == null) {
            this.spatialGranularity = collection.getSpatialGranularity();
        }
    }

    public ArrayList<String> createCSVRecords() {
        ArrayList<String> csvRecords = new ArrayList<String>();
        csvRecords.add(this.createHeaderRow());
        for (String spatialGranule : finalSpatialGranules) {
            csvRecords.add(this.createCsvRow(spatialGranule));
        }
        return csvRecords;
    }

    public String createHeaderRow() {
        ArrayList<String> headerRow = new ArrayList<>();
        headerRow.add("spatialID");
        headerRow.add("dtg");

        for (String featureType : featureTypeNames) {
            String attributeValueName = aggregatedAttributeNames.get(featureType);
            String header = featureType + "_" + attributeValueName;
            headerRow.add(header);
        }

        String joined = String.join(",", headerRow);
        return joined;
    }

    public String createCsvRow(String targetGranule) {
        ArrayList<String> row = new ArrayList<>();
        row.add(targetGranule);
        row.add(dtg);
        for (String featureType : featureTypeNames) {
            String aggregateAttr = aggregatedAttributeNames.get(featureType);
            SimpleFeature feature = aggregatedFeatures.get(featureType).getFeatureForSpatialGranule(targetGranule);
            if (feature != null) {
                String value = feature.getAttribute(aggregateAttr).toString();
                row.add(value);
            } else {
                row.add("NA");
            }
        }
        String joined = String.join(",", row);
        return joined;
    }

    public String getTemporalGranularity() {
        return temporalGranularity;
    }

    public String getSpatialGranularity() {
        return spatialGranularity;
    }

    public String getDtg() {
        return dtg;
    }

    public ArrayList<String> getFeatureTypeNames() {
        return featureTypeNames;
    }

    public ArrayList<String> getFinalSpatialGranules() {
        return finalSpatialGranules;
    }

    public HashMap<String, SpatioTemporallyAggregatedCollection> getAggregatedFeatures() {
        return aggregatedFeatures;
    }

    public HashMap<String, String> getAggregatedAttributeNames() {
        return aggregatedAttributeNames;
    }
}
