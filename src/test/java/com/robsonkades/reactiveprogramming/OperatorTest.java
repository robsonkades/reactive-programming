package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorTest.class);


    @Test
    public void subscribeOn() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log()
                .map(i -> {
                    LOGGER.info("Map 1, value {} thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    LOGGER.info("Map 2, value {} thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier
                .create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    public void publishOn() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log()
                .map(i -> {
                    LOGGER.info("Map 1, value {} thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    LOGGER.info("Map 2, value {} thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier
                .create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }
}
