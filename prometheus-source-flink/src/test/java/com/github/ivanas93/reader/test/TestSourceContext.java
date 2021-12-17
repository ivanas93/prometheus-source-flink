package com.github.ivanas93.reader.test;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TestSourceContext<T> implements SourceFunction.SourceContext<T> {

    private final List<T> list;
    private final Lock lock = new ReentrantLock();

    public TestSourceContext() {
        list = new ArrayList<>();
    }

    @Override
    public void collect(final T t) {
        try {
            lock.tryLock(2, TimeUnit.SECONDS);
            list.add(t);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public Iterable<T> iterator() {
        try {
            lock.tryLock(1, TimeUnit.SECONDS);
            return List.copyOf(list);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void collectWithTimestamp(final T t, final long l) {
        throw new NotImplementedException("This method is not required");
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
        throw new NotImplementedException("This method is not required");
    }

    @Override
    public void markAsTemporarilyIdle() {
        throw new NotImplementedException("This method is not required");
    }

    @Override
    public Object getCheckpointLock() {
        return this;
    }

    @Override
    public void close() {
        list.clear();
    }
}
