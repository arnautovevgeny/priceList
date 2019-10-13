package net.ddns.arnautovevgeny.pricelist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class StorageById implements AutoCloseable {
    private class Entry {
        private NavigableSet<Product> products = new ConcurrentSkipListSet<>();
        private AtomicInteger approximateSize = new AtomicInteger();

        private void reset() {
            this.products.clear();
            this.approximateSize.set(0);
        }

        private boolean add(Product product) {
            boolean added = this.products.add(product);
            if (added) {
                int count = this.approximateSize.incrementAndGet();
                if (count == limitById)
                    idsToShrink.offer(product.getId());
            }
            return added;
        }

        private boolean remove(Product product) {
            boolean removed = this.products.remove(product);
            if (removed)
                this.approximateSize.decrementAndGet();
            return removed;
        }

        private Product poll() {
            Product product = this.products.pollLast();
            if (product != null)
                this.approximateSize.decrementAndGet();

            return product;
        }

        private boolean isEmpty() {
            return this.products.isEmpty();
        }

        private int size() {
            return this.approximateSize.get();
        }
    }

    private class EntriesPool {
        private Queue<Entry> entriesPool = new ConcurrentLinkedQueue<>();

        private Entry get() {
            Entry entry = this.entriesPool.poll();
            if (entry == null)
                entry = new Entry();

            return entry;
        }

        private void add(Entry entry) {
            entry.reset();
            this.entriesPool.offer(entry);
        }

        private void clear() {
            this.entriesPool.clear();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StorageById.class);
    private static final int limitById = 20;

    private final Lock sharedLock;
    private final Lock exclusiveLock;

    private final Map<Integer, Entry> storage;

    private final EntriesPool entriesPool;
    private final Queue<Integer> idsToShrink;

    {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        sharedLock = readWriteLock.readLock();
        exclusiveLock = readWriteLock.writeLock();

        storage = new ConcurrentHashMap<>();

        entriesPool = new EntriesPool();
        idsToShrink = new ConcurrentLinkedQueue<>();
    }

    public boolean add(Product product)
    {
        log.debug("Adding product by id {}", product);

        this.sharedLock.lock();
        try {
            Entry newEntry = this.entriesPool.get();

            Entry entry = this.storage.putIfAbsent(product.getId(), newEntry);
            boolean isNew = entry == null;
            if (isNew)
                entry = newEntry;
            else
                this.entriesPool.add(newEntry);

            return entry.add(product);
        }
        finally {
            this.sharedLock.unlock();
        }
    }

    public void remove(Collection<Product> products) {
        this.exclusiveLock.lock();
        try {
            for (Product product : products) {
                Integer productId = product.getId();

                Entry entry = this.storage.get(productId);
                boolean isRemoved = entry.remove(product);
                if (isRemoved && entry.isEmpty()) {
                    this.entriesPool.add(entry);
                    this.storage.remove(productId);
                }
            }
        }
        finally {
            this.exclusiveLock.unlock();

            log.debug("{} elements was removed from productsById", products.size());
        }
    }

    public Collection<Product> shrink() {
        this.exclusiveLock.lock();
        try {
            List<Product> removed = new LinkedList<>();

            Integer id;
            while ((id = this.idsToShrink.poll()) != null) {
                Entry entry = this.storage.get(id);
                for (int i = entry.size(); i > limitById; i--) {
                    removed.add(entry.poll());
                }
            }

            return removed;
        }
        finally {
            this.exclusiveLock.unlock();
        }
    }

    @Override
    public void close() {
        this.exclusiveLock.lock();

        try {
            this.idsToShrink.clear();
            this.entriesPool.clear();
            this.storage.clear();
        }
        finally {
            this.exclusiveLock.unlock();
        }
    }
}
