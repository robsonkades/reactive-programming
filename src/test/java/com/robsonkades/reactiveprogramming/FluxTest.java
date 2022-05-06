package com.robsonkades.reactiveprogramming;

import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluxTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FluxTest.class);

    @BeforeAll
    static void setUp() {
        // Ignorar erros quando uma thread for bloqueada.
        BlockHound.install(builder -> builder.allowBlockingCallsInside("java.lang.Thread", "sleep"));
    }

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

        flux.subscribeWith(new BaseSubscriber<Integer>() {
            @Override
            protected void hookOnNext(Integer value) {
                LOGGER.info("Number {}", value);
            }


            @Override
            protected void hookOnError(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            protected void hookOnComplete() {
                LOGGER.info("Completed");
            }

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(3);
            }
        });

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
    public void fluxSubscriberNumbersLimitRate() {
        Flux<Integer> flux = Flux
                .range(1, 10)
                .log()
                .limitRate(3);

        flux.subscribe((e) -> LOGGER.info("Number {}", e));

        StepVerifier
                .create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

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

    @Test
    public void connectableFlux() throws InterruptedException {

        ConnectableFlux<Integer> publish = Flux
                .range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        publish.connect();

        Thread.sleep(300);

        publish.subscribe(i -> LOGGER.info("consumer 1 {}", i));

        Thread.sleep(300);

        publish.subscribe(i -> LOGGER.info("consumer 2 {}", i));

    }

    @Test
    public void connectableFluxVerify() throws InterruptedException {

        ConnectableFlux<Integer> publish = Flux
                .range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();


        StepVerifier
                .create(publish)
                .then(publish::connect)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();

        StepVerifier
                .create(publish)
                .then(publish::connect)
                .thenConsumeWhile(i -> i <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void connectableFluxAutoConnect() throws InterruptedException {
        Flux<Integer> flux = Flux
                .range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier
                .create(flux)
                .then(flux::subscribe)
                .expectNext(1,2,3,4,5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }
}
