package io.datasearch.diseasedata.store.sourceconnector.model;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Source Schema.
 */
public class DataPoint {
    /**
     * Schema for receiving events.
     */
    public static final String STATION_ID = "StationID";
    public static final String STATION_NAME = "StationName";
    public static final String LATITUDE = "Latitude";
    public static final String LONGITUDE = "Longitude";
    public static final String DATE = "dtg";
    public static final String OBSERVED_VALUE = "ObservedValue";
    public static final Schema SCHEMA = SchemaBuilder.struct()
            .name(DataPoint.class.getSimpleName())
            .field(STATION_NAME, Schema.STRING_SCHEMA)
            .field(LATITUDE, Schema.STRING_SCHEMA)
            .field(LONGITUDE, Schema.STRING_SCHEMA)
            .field(DATE, Schema.STRING_SCHEMA)
            .field(OBSERVED_VALUE, Schema.STRING_SCHEMA);


    private String stationID;
    private String stationName;
    private String latitude;
    private String longitude;
    private String dtg;
    private String observedValue;

    public DataPoint(String stationID,String stationName,
                     String latitude, String longitude,
                     String dtg, String observedValue) {
        this.stationID = stationID;
        this.stationName = stationName;
        this.latitude = latitude;
        this.longitude = longitude;
        this.dtg = dtg;
        this.observedValue = observedValue;
    }

    public String getStationId() {
        return stationID;
    }

    public String getStationName() {
        return stationName;
    }

    public String getLatitude() {
        return latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public String getDtg() {
        return dtg;
    }

    public String getObservedValue() {
        return observedValue;
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA)
                .put(STATION_ID, getStationName())
                .put(STATION_NAME, getStationName())
                .put(LATITUDE, getLatitude())
                .put(LONGITUDE, getLongitude())
                .put(DATE, getDtg())
                .put(OBSERVED_VALUE, getObservedValue());

        return struct;
    }
}
