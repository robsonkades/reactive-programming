package com.robsonkades.reactiveprogramming;

import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorTest.class);

    @BeforeAll
    static void setUp() {
        BlockHound.install();
    }

    @Test
    public void testBlockHound(){
        Mono.delay(Duration.ofSeconds(1))
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(it -> {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
                .block();
    }

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

    @Test
    public void concatOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concat = Flux
                .concat(flux1, flux2)
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    public void concatWithOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concat = flux1
                .concatWith(flux2)
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    public void concatOperatorError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(i -> {
                    if (i.equals("b")) {
                        throw new IllegalArgumentException("Error");
                    }
                    return i;
                });

        Flux<String> flux2 = Flux.just("c", "d");
        Flux<String> concat = Flux
                .concatDelayError(flux1, flux2)
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError()
                .verify();
    }

    @Test
    public void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d", "e", "f");

        Flux<String> concat = Flux
                .combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("BC", "BD", "BE", "BF")
                .expectComplete()
                .verify();
    }

    @Test
    public void combineLatestOperator2() {
        // Não tem como garantir a ordem
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> flux2 = Flux.just("c", "d", "e", "f");

        Flux<String> concat = Flux
                .combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("AF", "BF")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeOperator() {
        // Não espera terminar o primeiro flux para iniciar o proximo (rodando em threads paralelas)
        Flux<String> flux1 = Flux.just("c", "d", "e", "f").delayElements(Duration.ofMillis(10));
        Flux<String> flux2 = Flux.just("a", "b", "x", "N");

        Flux<String> concat = Flux
                .merge(flux1, flux2)
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "b", "x", "N", "c", "d", "e", "f")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeWithOperator() {
        // Não espera terminar o primeiro flux para iniciar o proximo (rodando em threads paralelas)
        Flux<String> flux1 = Flux.just("c", "d", "e", "f").delayElements(Duration.ofMillis(10));
        Flux<String> flux2 = Flux.just("a", "b", "x", "N");

        Flux<String> concat = flux1.mergeWith(flux2).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("a", "b", "x", "N", "c", "d", "e", "f")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeOperatorError() {
        // Não espera terminar o primeiro flux para iniciar o proximo (rodando em threads paralelas)
        Flux<String> flux1 = Flux.just("c", "d", "e", "f")
                .map(i -> {
                    if (i.equals("e")) {
                        throw new IllegalArgumentException(i);
                    }
                    return i;
                });

        Flux<String> flux2 = Flux.just("a", "b", "x", "N");
        Flux<String> concat = Flux
                .mergeDelayError(1, flux1, flux2)
                .log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("c", "d", "a", "b", "x", "N")
                .expectError()
                .verify();
    }

    @Test
    public void mergeSequencial() {
        Flux<String> flux1 = Flux.just("c", "d").delayElements(Duration.ofMillis(10));
        Flux<String> flux2 = Flux.just("a", "b");

        Flux<String> concat = Flux.mergeSequential(flux1, flux2, flux1).log();

        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("c", "d", "a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    public Flux<Object> empty() {
        return Flux.empty();
    }

    @Test
    public void flatMapOperator() {
        // Bem parecido com o merge, não consegue garantir a order dos elementos
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> map = flux.map(String::toUpperCase)
                .flatMap(this::findByName)
                .log();

        StepVerifier.create(map)
                .expectSubscription()
                .expectNext("NAME_1B", "NAME_2B", "NAME_1A", "NAME_2A")
                .expectComplete()
                .verify();
    }

    @Test
    public void flatMapSequencialOperator() {
        // Bem parecido com o merge, consegue garantir a order dos elementos
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> map = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName)
                .log();

        StepVerifier.create(map)
                .expectSubscription()
                .expectNext("NAME_1A", "NAME_2A", "NAME_1B", "NAME_2B")
                .expectComplete()
                .verify();
    }

    public Flux<String> findByName(String name) {
        return name.equals("A") ? Flux
                .just("NAME_1A", "NAME_2A")
                .delayElements(Duration.ofMillis(100)) :
                Flux.just("NAME_1B", "NAME_2B");
    }

    @Test
    public void zipOperator() {
        Flux<String> modelFlux = Flux.just("Gol", "Uno", "Fit");
        Flux<String> brandFlux = Flux.just("WV", "Fiat", "Honda");
        Flux<Integer> yearFlux = Flux.just(2019, 2018, 2022);

        Flux<Car> carFlux = Flux.zip(modelFlux, brandFlux, yearFlux)
                .flatMap(i -> Mono.just(new Car(i.getT1(), i.getT2(), i.getT3())))
                .log();

        StepVerifier.create(carFlux)
                .expectSubscription()
                .expectNext(
                        new Car("Gol", "WV", 2019),
                        new Car("Uno", "Fiat", 2018),
                        new Car("Fit", "Honda", 2022))
                .expectComplete()
                .verify();
    }

    @Test
    public void zipWithOperator() {
        // ZipWith so pode ter dois
        Flux<String> modelFlux = Flux.just("Gol", "Uno", "Fit");
        Flux<String> brandFlux = Flux.just("WV", "Fiat", "Honda");
        //Flux<Integer> yearFlux = Flux.just(2019, 2018, 2022);
        Flux<Car> carFlux = modelFlux
                .zipWith(brandFlux)
                .flatMap(i -> Mono.just(new Car(i.getT1(), i.getT2(), 0)))
                .log();

        StepVerifier.create(carFlux)
                .expectSubscription()
                .expectNext(
                        new Car("Gol", "WV", 0),
                        new Car("Uno", "Fiat", 0),
                        new Car("Fit", "Honda", 0))
                .expectComplete()
                .verify();
    }


    static class Car {
        private String model;
        private String brand;
        private int year;

        public Car(String model, String brand, int year) {
            this.model = model;
            this.brand = brand;
            this.year = year;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Car car = (Car) o;

            if (year != car.year) return false;
            if (!Objects.equals(model, car.model)) return false;
            return Objects.equals(brand, car.brand);
        }

        @Override
        public int hashCode() {
            int result = model != null ? model.hashCode() : 0;
            result = 31 * result + (brand != null ? brand.hashCode() : 0);
            result = 31 * result + year;
            return result;
        }
    }
}
