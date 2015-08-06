/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.RuntimePairComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.operators.testutils.*;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractOuterJoinTaskTest extends BinaryOperatorTestBase<FlatJoinFunction<Tuple2<Integer, Integer>,
		Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	private static final long HASH_MEM = 6*1024*1024;

	private static final long SORT_MEM = 3*1024*1024;

	private static final int NUM_SORTER = 2;

	private static final long BNLJN_MEM = 10 * PAGE_SIZE;

	private final double bnljn_frac;

	private final DriverStrategy driverStrategy;

	@SuppressWarnings("unchecked")
	private final TypeComparator<Tuple2<Integer, Integer>> comparator1 = new TupleComparator<>(
			new int[]{0},
			new TypeComparator<?>[] { new IntComparator(true) },
			new TypeSerializer<?>[] { IntSerializer.INSTANCE });

	@SuppressWarnings("unchecked")
	private final TypeComparator<Tuple2<Integer, Integer>> comparator2 = new TupleComparator<>(
			new int[]{0},
			new TypeComparator<?>[] { new IntComparator(true) },
			new TypeSerializer<?>[] { IntSerializer.INSTANCE });

	private final List<Tuple2<Integer, Integer>> outList = new ArrayList<>();

	private final TypeSerializer<Tuple2<Integer, Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
			new TypeSerializer<?>[] { IntSerializer.INSTANCE, IntSerializer.INSTANCE });


	public AbstractOuterJoinTaskTest(ExecutionConfig config, DriverStrategy driverStrategy) {
		super(config, HASH_MEM, NUM_SORTER, SORT_MEM);
		bnljn_frac = (double)BNLJN_MEM/this.getMemoryManager().getMemorySize();
		this.driverStrategy = driverStrategy;
	}

	@Test
	public void testSortBoth1OuterJoinTask() {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;
		
		final int keyCnt2 = 10;
		final int valCnt2 = 2;

		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testSortBoth2OuterJoinTask() {
		final int keyCnt1 = 20;
		final int valCnt1 = 1;

		final int keyCnt2 = 20;
		final int valCnt2 = 1;

		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testSortBoth3OuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 1;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testSortBoth4OuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 1;

		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testSortBoth5OuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	@Test
	public void testSortBoth6OuterJoinTask() {
		int keyCnt1 = 10;
		int valCnt1 = 1;

		int keyCnt2 = 20;
		int valCnt2 = 2;

		testSortBothOuterJoinTask(keyCnt1, valCnt1, keyCnt2, valCnt2);
	}

	private void testSortBothOuterJoinTask(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2) {
		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		try {
			addInputSorted(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), serializer, this.comparator1.duplicate());
			addInputSorted(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), serializer, this.comparator2.duplicate());
			testDriver(testTask, MockJoinStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}

		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);
		Assert.assertTrue("Resultset size was " + this.outList.size() + ". Expected was " + expCnt, this.outList.size() == expCnt);

		this.outList.clear();
	}

	@Test
	public void testSortFirstOuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		try {
			addInputSorted(new UniformIntTupleGenerator(keyCnt1, valCnt1, false), serializer, this.comparator1.duplicate());
			addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), serializer);
			testDriver(testTask, MockJoinStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}

		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);

		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);

		this.outList.clear();
	}

	@Test
	public void testSortSecondOuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		try {
			addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), serializer);
			addInputSorted(new UniformIntTupleGenerator(keyCnt2, valCnt2, false), serializer, this.comparator2.duplicate());
			testDriver(testTask, MockJoinStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}

		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);

		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);

		this.outList.clear();
	}

	@Test
	public void testMergeOuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		setOutput(this.outList, this.serializer);
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), serializer);

		try {
			testDriver(testTask, MockJoinStub.class);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}

		final int expCnt = calculateExpectedCount(keyCnt1, valCnt1, keyCnt2, valCnt2);

		Assert.assertTrue("Resultset size was "+this.outList.size()+". Expected was "+expCnt, this.outList.size() == expCnt);

		this.outList.clear();
	}

	@Test
	public void testFailingOuterJoinTask() {
		int keyCnt1 = 20;
		int valCnt1 = 20;

		int keyCnt2 = 20;
		int valCnt2 = 20;

		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		addInput(new UniformIntTupleGenerator(keyCnt1, valCnt1, true), serializer);
		addInput(new UniformIntTupleGenerator(keyCnt2, valCnt2, true), serializer);

		try {
			testDriver(testTask, MockFailingJoinStub.class);
			Assert.fail("Driver did not forward Exception.");
		} catch (ExpectedTestException e) {
			// good!
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}
	}

	@Test
	public void testCancelOuterJoinTaskWhileSort1() {
		int keyCnt = 20;
		int valCnt = 20;

		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		try {
			addInputSorted(new DelayingIterator<>(new InfiniteIntTupleIterator(), 1), this.serializer, this.comparator1.duplicate());
			addInput(new UniformIntTupleGenerator(keyCnt, valCnt, true), serializer);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}

		final AtomicBoolean success = new AtomicBoolean(false);

		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockJoinStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		taskRunner.start();

		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
		tct.start();

		try {
			tct.join();
			taskRunner.join();
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}

		Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
	}

	@Test
	public void testCancelOuterJoinTaskWhileSort2() {
		int keyCnt = 20;
		int valCnt = 20;

		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();

		try {
			addInput(new UniformIntTupleGenerator(keyCnt, valCnt, true), serializer);
			addInputSorted(new DelayingIterator<>(new InfiniteIntTupleIterator(), 1), this.serializer, this.comparator2.duplicate());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("The test caused an exception.");
		}

		final AtomicBoolean success = new AtomicBoolean(false);

		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockJoinStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		taskRunner.start();

		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
		tct.start();

		try {
			tct.join();
			taskRunner.join();
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}

		Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
	}

	@Test
	public void testCancelOuterJoinTaskWhileRunning() {
		int keyCnt = 20;
		int valCnt = 20;

		setOutput(new DiscardingOutputCollector<Tuple2<Integer, Integer>>());
		addDriverComparator(this.comparator1);
		addDriverComparator(this.comparator2);
		getTaskConfig().setDriverPairComparator(new RuntimePairComparatorFactory());
		getTaskConfig().setDriverStrategy(driverStrategy);
		getTaskConfig().setRelativeMemoryDriver(bnljn_frac);
		setNumFileHandlesForSort(4);

		final AbstractOuterJoinDriver<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> testTask =
				getOuterJoinDriver();


		addInput(new UniformIntTupleGenerator(keyCnt, valCnt, true), serializer);
		addInput(new UniformIntTupleGenerator(keyCnt, valCnt, true), serializer);

		final AtomicBoolean success = new AtomicBoolean(false);

		Thread taskRunner = new Thread() {
			@Override
			public void run() {
				try {
					testDriver(testTask, MockDelayingJoinStub.class);
					success.set(true);
				} catch (Exception ie) {
					ie.printStackTrace();
				}
			}
		};
		taskRunner.start();

		TaskCancelThread tct = new TaskCancelThread(1, taskRunner, this);
		tct.start();

		try {
			tct.join();
			taskRunner.join();
		} catch(InterruptedException ie) {
			Assert.fail("Joining threads failed");
		}

		Assert.assertTrue("Test threw an exception even though it was properly canceled.", success.get());
	}

	protected abstract AbstractOuterJoinDriver<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>> getOuterJoinDriver();

	protected abstract int calculateExpectedCount(int keyCnt1, int valCnt1, int keyCnt2, int valCnt2);

	// =================================================================================================
	
	public static final class MockJoinStub implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			out.collect(first != null ? first : second);
		}
	}
	
	public static final class MockFailingJoinStub implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		private int cnt = 0;

		@Override
		public void join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			if (++this.cnt >= 10) {
				throw new ExpectedTestException();
			}
			out.collect(first != null ? first : second);
		}
	}
	
	public static final class MockDelayingJoinStub implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}
}
