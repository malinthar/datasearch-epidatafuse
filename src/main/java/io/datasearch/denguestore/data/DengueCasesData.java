package io.datasearch.denguestore.data;

import io.datasearch.denguestore.DiseaseData;
import io.datasearch.denguestore.DiseaseDataStore;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * DengueCases Class .
 */
public class DengueCasesData implements DiseaseData {

    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private List<Query> queries = null;
    private Filter subsetFilter = null;
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    @Override
    public String getTypeName() {
        return "dengue-data-old";
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        if (sft == null) {
            // list the attributes that constitute the feature type
            // this is a reduced set of the attributes from GDELT 2.0
            StringBuilder attributes = new StringBuilder();
            attributes.append("Locale:String,"); // marks this attribute for indexing
            attributes.append("Latitude:String,");
            attributes.append("Longitude:String,");
            attributes.append("resident:String,");
            attributes.append("work:String:index=true,");
            attributes.append("*geom:Point:srid=4326"); // the "*" denotes the default geometry (used for indexing)

            // create the simple-feature type - use the GeoMesa 'SimpleFeatureTypes' class for best compatibility
            // may also use geotools DataUtilities or SimpleFeatureTypeBuilder, but some features may not work
            sft = SimpleFeatureTypes.createType(getTypeName(), attributes.toString());

            // use the user-data (hints) to specify which date field to use for primary indexing
            // if not specified, the first date attribute (if any) will be used
            // could also use ':default=true' in the attribute specification string
            //sft.getUserData().put(SimpleFeatureTypes.DEFAULT_DATE_KEY, "dtg");
        }
        return sft;
    }

    @Override
    public List<SimpleFeature> getTestData() {
        if (features == null) {
            List<SimpleFeature> features = new ArrayList<>();

            // read the bundled GDELT 2.0 TSV
            URL input = getClass().getClassLoader().getResource("cdr/cdr-data.csv");
            if (input == null) {
                throw new RuntimeException("Couldn't load resource cdr-data.CSV");
            }

            // parser corresponding to the CSV format
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            // use a geotools SimpleFeatureBuilder to create our features
            // use apache commons-csv to parse the GDELT file
            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
                for (CSVRecord record : parser) {
                    try {
                        // pull out the fields corresponding to our simple feature attributes
                        builder.set("Locale", record.get(0));
                        builder.set("Latitude", record.get(1));
                        builder.set("Longitude", record.get(2));
                        builder.set("resident", record.get(3));
                        builder.set("work", record.get(4));
                        // we can use WKT (well-known-text) to represent geometries
                        // note that we use longitude first ordering
                        double latitude = Double.parseDouble(record.get(1));
                        double longitude = Double.parseDouble(record.get(2));
                        builder.set("geom", "POINT (" + longitude + " " + latitude + ")");

                        // be sure to tell GeoTools explicitly that we want to use the ID we provided
                        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

                        // build the feature - this also resets the feature builder for the next entry
                        // use the Location as the feature ID
                        SimpleFeature feature = builder.buildFeature(record.get(0));
                        features.add(feature);
                    } catch (Throwable e) {
                        logger.debug("Invalid cdr-data record: " + e.toString() + " " + record.toString());
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading cdr-data:", e);
            }
            this.features = Collections.unmodifiableList(features);
        }
        return features;
    }

    @Override
    public List<Query> getTestQueries() {
        if (queries == null) {
            try {
                List<Query> queries = new ArrayList<>();

                // attribute query on a secondary index - note we specified index=true for Locale
                queries.add(new Query(getTypeName(), ECQL.toFilter("Locale = '1610'")));
                // attribute query on a secondary index with a projection

                this.queries = Collections.unmodifiableList(queries);
            } catch (CQLException e) {
                throw new RuntimeException("Error creating filter:", e);
            }
        }
        return queries;
    }

    @Override
    public Filter getSubsetFilter() {
        if (subsetFilter == null) {
            // Get a FilterFactory2 to build up our query
            FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

            // most of the data is from 2018-01-01
            ZonedDateTime dateTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            Date start = Date.from(dateTime.minusDays(1).toInstant());
            Date end = Date.from(dateTime.plusDays(1).toInstant());

            // note: BETWEEN is inclusive, while DURING is exclusive
            Filter dateFilter = ff.between(ff.property("dtg"), ff.literal(start), ff.literal(end));

            // bounding box over small portion of the eastern United States
            Filter spatialFilter = ff.bbox("geom", -83, 33, -80, 35, "EPSG:4326");

            // Now we can combine our filters using a boolean AND operator
            subsetFilter = ff.and(dateFilter, spatialFilter);

            // note the equivalent using ECQL would be:
            // ECQL.toFilter("bbox(geom,-83,33,-80,35) AND dtg between
            // '2017-12-31T00:00:00.000Z' and '2018-01-02T00:00:00.000Z'");
        }
        return subsetFilter;
    }
}
