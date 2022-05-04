package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluxTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FluxTest.class);

    @Test
    public void fluxSubscriber() {
        Flux<String> flux = Flux
                .just("Robbson", "Kades", "Daiane", "Darosci")
                .log();

        StepVerifier
                .create(flux)
                .expectNext("Robbson", "Kades", "Daiane", "Darosci")
                .verifyComplete();

    }

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

    @Test
    public void fluxSubscriberFromIterable() {
        Flux<Integer> flux = Flux
                .fromIterable(List.of(1, 2,3,4, 5))
                .log();

        flux.subscribe((e) -> LOGGER.info("Number {}", e));

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> flux = Flux
                .range(1, 5)
                .log()
                .map(i -> {
                    if (i == 4) {
                        throw new IndexOutOfBoundsException("Error -> IndexOutOfBoundsException");
                    }
                    return i;
                });

        flux.subscribe(
                e -> LOGGER.info("Number {}", e),
                Throwable::printStackTrace,
                () -> LOGGER.info("Completed"));

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();

    }

    @Test
    public void fluxSubscriberNumbersWithBackPressure() {
        Flux<Integer> flux = Flux
                .range(1, 5)
                .log()
                .map(i -> {
                    if (i == 4) {
                        throw new IndexOutOfBoundsException("Error -> IndexOutOfBoundsException");
                    }
                    return i;
                });

        flux.subscribe(
                e -> LOGGER.info("Number {}", e),
                Throwable::printStackTrace,
                () -> LOGGER.info("Completed"),
                subscription -> subscription.request(3));

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();

    }

    @Test
    public void fluxSubscriberNumbersWithBackPressureInPairs() {
        Flux<Integer> flux = Flux
                .range(1, 10)
                .log();

        flux.subscribe(new Subscriber<Integer>() {

            private int count = 0;
            private Subscription subscription;
            private final int requestCount = 2;
            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                this.subscription.request(requestCount);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    this.subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });

        LOGGER.info(" ------------------------ ");

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberNumbersWithBackPressureInPairsBaseSubscriber() {
        Flux<Integer> flux = Flux
                .range(1, 10)
                .log();

        flux.subscribe(new BaseSubscriber<Integer>() {

            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        LOGGER.info(" ------------------------ ");

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

    }

    @Test
    public void fluxSubscriberInterval() throws InterruptedException {
        Flux<Long> flux = Flux
                .interval(Duration.ofMillis(100))
                .log();

        flux.subscribe(e -> LOGGER.info("NUMBER {}", e));

        Thread.sleep(1000);
    }

    @Test
    public void fluxSubscriberIntervalTake() throws InterruptedException {
        Flux<Long> flux = Flux
                .interval(Duration.ofMillis(100))
                .take(3)
                .log();

        flux.subscribe(e -> LOGGER.info("NUMBER {}", e));

        Thread.sleep(1000);
    }

    @Test
    public void fluxSubscriberIntervalVirtualTime() throws InterruptedException {

        StepVerifier.withVirtualTime(() -> Flux
                .interval(Duration.ofDays(1))
                .log())
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }
}
