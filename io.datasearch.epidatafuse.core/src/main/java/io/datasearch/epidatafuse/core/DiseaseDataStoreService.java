package io.datasearch.epidatafuse.core;

import org.apache.log4j.BasicConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * DiseaseDataStore is the main class of the DataStore.
 */
@SpringBootApplication
public class DiseaseDataStoreService extends SpringBootServletInitializer {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(DiseaseDataStoreService.class);
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        SpringApplication.run(DiseaseDataStoreService.class, args);
    }
}
