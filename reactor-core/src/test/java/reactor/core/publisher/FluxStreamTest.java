/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxStreamTest {

	final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

	@SuppressWarnings("ConstantConditions")
	@Test(expected = NullPointerException.class)
	public void nullStream() {
		Flux.fromStream((Stream) null);
	}

	@SuppressWarnings("ConstantConditions")
	@Test(expected = NullPointerException.class)
	public void nullSupplier() {
		Flux.fromStream((Supplier<Stream<?>>) null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromStream(source.stream())
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.fromStream(source.stream())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredExact() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(10);

		Flux.fromStream(source.stream())
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void iteratorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromStream(Arrays.asList(1, 2, 3, 4, 5, null, 7, 8, 9, 10)
		                      .stream())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void streamAlreadyConsumed() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Stream<Integer> s = source.stream();

		s.count();

		Flux.fromStream(s)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(IllegalStateException.class);
	}

	@Test
	public void streamConsumedBySubscription() {
		Stream<Integer> stream = source.stream();
		Flux<Integer> flux = Flux.fromStream(stream);

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .verifyComplete();

		StepVerifier.create(flux)
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void streamGeneratedPerSubscription() {
		Flux<Integer> flux = Flux.fromStream(source::stream);

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .verifyComplete();

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .verifyComplete();
	}

	@Test
	public void nullSupplierErrorsAtSubscription() {
		Flux<String> flux = new FluxStream<>(() -> null);

		StepVerifier.create(flux)
		            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(NullPointerException.class)
				            .hasMessage("The stream supplier returned a null Stream"));
	}

}
