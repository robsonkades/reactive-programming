package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.Mono;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExampleTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExampleTest.class);

    @Test
    public void createFistReactive() {
        Mono<String> response = Mono.just("Hello Reactor");
        Assertions.assertEquals("Hello Reactor", response.block());
    }
}
