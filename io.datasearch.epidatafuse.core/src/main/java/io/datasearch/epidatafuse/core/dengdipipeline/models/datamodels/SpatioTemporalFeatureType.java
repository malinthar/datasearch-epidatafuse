package io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Data model for aggregated features
 */

public class SpatioTemporalFeatureType extends AbstractSpatioTemporalFeatureType {

    private SimpleFeatureType featureType;
    private SimpleFeatureCollection featureCollection;
    private String temporalGranularity;
    private String spatialGranularity;

    public SpatioTemporalFeatureType(SimpleFeatureType featureType,
                                     SimpleFeatureCollection featureCollection,
                                     String temporalGranularity, String spatialGranularity) {
        super(featureType, featureCollection, temporalGranularity, spatialGranularity);
    }

    public SimpleFeatureType getFeatureType() {
        return featureType;
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

    public String getFeatureTypeName() {
        return this.featureType.getTypeName();
    }
}
