package org.apache.pulsar.client.impl.weight;

import org.apache.pulsar.client.api.Message;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Specialized collection used when clearing incoming message queue in a multi consumer is required
 * Instead, the queue is drained to this collection which tracks the count of messages for every topic
 */
public class TopicMessageCountTracker implements Collection<Message> {
    Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();

    public Map<String, Integer> getCounters() {
        Map<String, Integer> ret = counters.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().get()));
        return Collections.unmodifiableMap(ret);
    }

    public boolean add(Message m) {
        counters.computeIfAbsent(m.getTopicName(), (k -> new AtomicInteger()));
        counters.get(m.getTopicName()).incrementAndGet();
        return true;
    }

    public boolean addAll(Collection<? extends Message> c) {
        c.stream().forEach(m -> add(m));
        return true;
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    public Iterator<Message> iterator() {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public void clear() {
        counters.clear();
    }
}
