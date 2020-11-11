package io.datasearch.epidatafuse.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testng.annotations.Test;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = RequestHandler.class)
@WebAppConfiguration
public class RequestHandlerTestCase extends AbstractTestNGSpringContextTests {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandlerTestCase.class.getName());

    @Autowired
    private WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Test
    public void testInit() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
        try {
            mockMvc.perform(get("/testinit")).andExpect(status().isOk());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
