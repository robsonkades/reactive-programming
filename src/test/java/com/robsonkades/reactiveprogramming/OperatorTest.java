package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Assertions;
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

    @Test
    public void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    LOGGER.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    LOGGER.info("Map 2- Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(i -> {
                    LOGGER.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    LOGGER.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(i -> {
                    LOGGER.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    LOGGER.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    LOGGER.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    LOGGER.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void subscribeOnIO() throws InterruptedException {
        Mono<List<String>> mono = Mono
                .fromCallable(() -> Files.readAllLines(Path.of("demo-io.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        mono.subscribe(s -> LOGGER.info("LINE {}", s));

        Thread.sleep(2000);

        StepVerifier.create(mono)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
                    return true;
                })
                .verifyComplete();
    }


    @Test
    public void switchIfEmpty() {
        Flux<Object> flux = empty()
                .switchIfEmpty(Flux.just("switchIfEmpty"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("switchIfEmpty")
                .expectComplete()
                .verify();
    }

    @Test
    public void defer() throws InterruptedException {
        //Mono<Long> mono = Mono.just(System.currentTimeMillis());
        Mono<Long> mono = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        mono.subscribe(s -> LOGGER.info("ERROR {}", s));
        Thread.sleep(100);

        mono.subscribe(s -> LOGGER.info("ERROR {}", s));
        Thread.sleep(100);

        mono.subscribe(s -> LOGGER.info("ERROR {}", s));
        Thread.sleep(100);

        mono.subscribe(s -> LOGGER.info("ERROR {}", s));
        Thread.sleep(100);

        AtomicLong atomicLong = new AtomicLong();
        mono.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    public Flux<Object> empty() {
        return Flux.empty();
    }

}
