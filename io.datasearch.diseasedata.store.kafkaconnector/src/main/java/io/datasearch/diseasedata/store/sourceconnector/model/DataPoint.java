package io.datasearch.diseasedata.store.sourceconnector.model;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Source Schema.
 */
public class DataPoint {
    /**
     * Schema for recieveing events.
     */
    public static final String TEST_STRING = "add";
    public static final Schema SCHEMA = SchemaBuilder.struct()
            .name(DataPoint.class.getSimpleName())
            .field(TEST_STRING, Schema.STRING_SCHEMA);

    private String add;

    public DataPoint(String add) {
        this.add = add;
    }

    public String getTestString() {
        return add;
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA)
                .put(TEST_STRING, getTestString());
        return struct;
    }
}
