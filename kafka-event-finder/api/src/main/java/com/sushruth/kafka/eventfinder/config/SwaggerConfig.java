package com.sushruth.kafka.eventfinder.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import springfox.documentation.annotations.ApiIgnore;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.time.LocalDate;
import java.util.Collections;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Bean(value = "api")
    public Docket apiParty() {
    return new Docket(DocumentationType.SWAGGER_2)
        .select()
        .apis(
            RequestHandlerSelectors.basePackage("com.sushruth.kafka.eventfinder.controller"))
            .paths(PathSelectors.any())
            .build()
        .apiInfo(apiInfo())
            .pathMapping("/")
        .directModelSubstitute(LocalDate .class, String.class)
        .genericModelSubstitutes(ResponseEntity .class)
        .ignoredParameterTypes(ApiIgnore .class);
}

    private ApiInfo apiInfo() {
        return new ApiInfo(
                "Kafka Event Finder",
                "search for an event by header or offset",
                "1.0",
                "",
                ApiInfo.DEFAULT_CONTACT,
                "",
                "",
                Collections.emptyList());
    }
}
