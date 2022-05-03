package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluxTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FluxTest.class);

    @Test
    public void fluxSubscriberNumbers() {
        Flux<Integer> flux = Flux
                .range(1, 5)
                .log();

        flux.subscribe((e) -> LOGGER.info("Number {}", e));

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

    }
}
