package org.apache.flink.api.java.typeutils.runtime;

public interface TupleCreator<T> {

	T createInstance(Object[] fields);
}
