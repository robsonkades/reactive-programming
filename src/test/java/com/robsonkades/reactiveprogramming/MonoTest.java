package com.robsonkades.reactiveprogramming;

import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MonoTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonoTest.class);

    @BeforeAll
    static void setUp() {
        BlockHound.install();
    }

    @Test
    public void monoSubscriber() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .log();

        mono.subscribe();

        LOGGER.info("---------------------");

        StepVerifier.create(mono)
                .expectNext("Hello Reactor")
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .log();

        mono.subscribe(s -> LOGGER.info("Consumer {}", s));

        LOGGER.info("---------------------");

        StepVerifier.create(mono)
                .expectNext("Hello Reactor")
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerError() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .map(s -> { throw new RuntimeException("Testing with mono error"); });

        mono.subscribe(
                s -> LOGGER.info("Consumer {}", s),
                e -> LOGGER.error("ERROR {}", e.getMessage()));

        mono.subscribe(
                s -> LOGGER.info("Consumer {}", s),
                Throwable::printStackTrace);

        LOGGER.info("---------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    public void monoSubscriberConsumerComplete() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .log()
                .map(String::toUpperCase);

        mono.subscribe(
                s -> LOGGER.info("Consumer {}", s),
                Throwable::printStackTrace,
                () -> LOGGER.info("FINISHED!"));

        LOGGER.info("---------------------");

        StepVerifier.create(mono)
                .expectNext("HELLO REACTOR")
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerSubscription() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .log()
                .map(String::toUpperCase);

        mono.subscribe(
                s -> LOGGER.info("Consumer {}", s),
                Throwable::printStackTrace,
                () -> LOGGER.info("FINISHED!"),
                subscription -> subscription.request(5));

        LOGGER.info("---------------------");

        StepVerifier.create(mono)
                .expectNext("HELLO REACTOR")
                .verifyComplete();
    }

    @Test
    public void monoDoOnMethod() {
        Mono<String> mono = Mono
                .just("Hello Reactor")
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> LOGGER.info("subscribed"))
                .doOnRequest(value -> LOGGER.info("Request {}", value))
                .doOnNext(s -> LOGGER.info("OnNext {}", s))
                .doOnNext(s -> LOGGER.info("OnNext {}", s))
                .flatMap(s -> Mono.just("ABC"))
                .doOnNext(s -> LOGGER.info("OnNext {}", s))
                .doOnNext(s -> LOGGER.info("OnNext {}", s))
                .doOnSuccess(s -> LOGGER.info("OnSuccess {}", s));

        mono.subscribe(
                s -> LOGGER.info("Consumer {}", s),
                Throwable::printStackTrace,
                () -> LOGGER.info("FINISHED!"),
                subscription -> subscription.request(5));

        LOGGER.info("---------------------");

//        StepVerifier.create(mono)
//                .expectNext("HELLO REACTOR")
//                .verifyComplete();
    }

    @Test
    public void monoOnError() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("On Error demo"))
                .doOnError(s -> LOGGER.info("OnError message {}", s.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();

    }

    @Test
    public void monoOnErrorOnResume() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("On Error demo"))
                .doOnError(s -> LOGGER.info("OnError message {}", s.getMessage()))
                .onErrorResume(s -> Mono.just("Fallback"))
                .log();

        StepVerifier.create(error)
                .expectNext("Fallback")
                .verifyComplete();

    }

    @Test
    public void monoOnErrorOnReturn() {
        Mono<Object> error = Mono.error(new IllegalArgumentException("On Error demo"))
                .doOnError(s -> LOGGER.info("OnError message {}", s.getMessage()))
                .onErrorReturn("EMPTY")
                .onErrorResume(s -> Mono.just("Fallback"))
                .log();

        StepVerifier.create(error)
                .expectNext("EMPTY")
                .verifyComplete();

    }

    @Test
    public void monoWithCache () throws InterruptedException {
        Mono<Integer> mono = Mono
                .just(1)
                .map(i -> {
                    LOGGER.info("Step: 1 - without cache");
                    return i + 1;
                })
                .map(i -> {
                    LOGGER.info("Step: 2 - without cache");
                    return i + 1;
                })
                .cache(Duration.ofMillis(100))
                .log();
        StepVerifier.create(mono)
                .expectSubscription()
                .expectNext(3)
                .expectComplete()
                .verify();
    }
}
