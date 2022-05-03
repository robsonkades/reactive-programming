package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MonoTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonoTest.class);

    @Test
    public void monoSubscribe() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .log();

        mono.subscribe();

        LOGGER.info("---------------------");

        StepVerifier.create(mono)
                .expectNext("Hello Reactor")
                .verifyComplete();
    }
}
