package io.datasearch.diseasedata.store;

import org.apache.log4j.BasicConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * DiseaseDataStore is the main class of the DataStore.
 */
@SpringBootApplication
public class DiseaseDataStore extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(DiseaseDataStore.class);
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        SpringApplication.run(DiseaseDataStore.class, args);
    }
}
