package com.robsonkades.reactiveprogramming;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MonoTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonoTest.class);

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
}
