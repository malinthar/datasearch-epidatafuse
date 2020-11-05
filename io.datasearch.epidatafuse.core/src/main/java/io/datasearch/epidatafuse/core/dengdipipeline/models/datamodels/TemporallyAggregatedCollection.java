package io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;

/**
 * Data model for temporal aggregated feature collection
 */

public class TemporallyAggregatedCollection implements AggregatedCollection {
    private SimpleFeatureType featureType;
    private SimpleFeatureCollection featureCollection;
    private String spatialGranularity;
    private String temporalGranularity;
    private String dtg;

    public TemporallyAggregatedCollection(SimpleFeatureType featureType, SimpleFeatureCollection featureCollection,
                                          String spatialGranularity,
                                          String temporalGranularity,
                                          String dtg) {
        this.featureType = featureType;
        this.featureCollection = featureCollection;
        this.spatialGranularity = spatialGranularity;
        this.temporalGranularity = temporalGranularity;
        this.dtg = dtg;
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

    @Override
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
        return null;
    }
}
