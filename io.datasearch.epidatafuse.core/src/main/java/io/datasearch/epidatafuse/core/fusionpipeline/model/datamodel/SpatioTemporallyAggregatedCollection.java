package io.datasearch.epidatafuse.core.fusionpipeline.model.datamodel;


import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Data model for temporal aggregated feature collection
 */

public class SpatioTemporallyAggregatedCollection implements AggregatedCollection {
    private SimpleFeatureType featureType;
    private SimpleFeatureCollection featureCollection;
    private String spatialGranularity;
    private String temporalGranularity;
    private String dtg;
    private ArrayList<String> attributeList;
    private ArrayList<String> aggregatedAttributeList;

    private HashMap<String, SimpleFeature> featureHashMap = new HashMap<String, SimpleFeature>();

    public SpatioTemporallyAggregatedCollection(SimpleFeatureType featureType,
                                                SimpleFeatureCollection featureCollection,
                                                String spatialGranularity,
                                                String temporalGranularity, String dtg,
                                                ArrayList<String> attributeList,
                                                ArrayList<String> aggregatedAttributeList) {
        this.featureType = featureType;
        this.featureCollection = featureCollection;
        this.spatialGranularity = spatialGranularity;
        this.temporalGranularity = temporalGranularity;
        this.dtg = dtg;
        this.attributeList = attributeList;
        this.aggregatedAttributeList = aggregatedAttributeList;

        SimpleFeatureIterator iterator = featureCollection.features();
        while (iterator.hasNext()) {
            SimpleFeature next = iterator.next();
            featureHashMap.put(next.getID(), next);
        }

    }

    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    public String getFeatureTypeName() {
        return featureType.getTypeName();
    }

    public SimpleFeatureCollection getFeatureCollection() {
        return featureCollection;
    }

    public String getSpatialGranularity() {
        return spatialGranularity;
    }

    public String getTemporalGranularity() {
        return temporalGranularity;
    }

    public String getDtg() {
        return dtg;
    }

    @Override
    public ArrayList<String> getAttributeList() {
        return attributeList;
    }

    public HashMap<String, SimpleFeature> getFeatureHashMap() {
        return featureHashMap;
    }

    public ArrayList<String> getAggregatedAttributeList() {
        return aggregatedAttributeList;
    }

    public SimpleFeature getFeatureForSpatialGranule(String spatialGranule) {
        if (this.featureHashMap.containsKey(spatialGranule)) {
            return this.featureHashMap.get(spatialGranule);
        } else {
            return null;
        }

    }
}
