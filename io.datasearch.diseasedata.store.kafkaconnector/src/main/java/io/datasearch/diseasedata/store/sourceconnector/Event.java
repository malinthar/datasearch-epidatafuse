package io.datasearch.diseasedata.store.sourceconnector;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Source Schema.
 */
public class Event {
    /**
     * Schema for recieveing events.
     */
    public static final String TEST_STRING = "TestString";
    public static final Schema SCHEMA = SchemaBuilder.struct()
            .name("DiseaseDataSource1")
            .field(TEST_STRING, Schema.STRING_SCHEMA);

    private String testString;

    public Event(String testString) {
        this.testString = testString;
    }

    public String getTestString() {
        return testString;
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA)
                .put(TEST_STRING, getTestString());
        return struct;
    }
}
