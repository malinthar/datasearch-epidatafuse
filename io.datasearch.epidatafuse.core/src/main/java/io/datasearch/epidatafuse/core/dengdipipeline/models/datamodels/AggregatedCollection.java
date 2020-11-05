package io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

import java.util.ArrayList;

/**
 * Data model for aggregated features
 */

public interface AggregatedCollection {
    public String getFeatureTypeName();

    public SimpleFeatureType getFeatureType();

    public SimpleFeatureCollection getFeatureCollection();

    public String getDtg();

    public String getSpatialGranularity();

    public String getTemporalGranularity();

    public ArrayList<String> getAttributeList();

}

