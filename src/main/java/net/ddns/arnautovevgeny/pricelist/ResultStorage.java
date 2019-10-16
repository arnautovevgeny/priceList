package net.ddns.arnautovevgeny.pricelist;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ResultStorage implements AutoCloseable {
    private class EntryById {
        private NavigableSet<Product> products = new ConcurrentSkipListSet<>();
        private AtomicInteger approximateSize = new AtomicInteger();

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

    private class EntriesByIdPool {
        private Queue<EntryById> entriesPool = new ConcurrentLinkedQueue<>();

        private EntryById get() {
            EntryById entryById = this.entriesPool.poll();
            if (entryById == null)
                entryById = new EntryById();

            return entryById;
        }

        private void add(EntryById entryById) {
            this.entriesPool.offer(entryById);
        }

        private void clear() {
            this.entriesPool.clear();
        }
    }

    private static final int limitTotal = 1000;
    private static final long operationsToCleanUp = 1000;
    private static final long outputProceedCount = 10000;

    private final NavigableSet<Product> storageTotal;
    private final AtomicInteger size;
    private final AtomicLong operationsCounter;

    private final AtomicLong readTotal;
    private final AtomicLong proceedTotal;

    private final AtomicBoolean stopped;
    private final AtomicBoolean ready;

    private static final int limitById = 20;

    private final Map<Integer, EntryById> storageById;

    private final EntriesByIdPool entriesByIdPool;
    private final Queue<Integer> idsToShrink;

    private final AtomicBoolean removingInProgress;

    {
        storageTotal = new ConcurrentSkipListSet<>();
        size = new AtomicInteger();
        operationsCounter = new AtomicLong();

        readTotal = new AtomicLong();
        proceedTotal = new AtomicLong();

        stopped = new AtomicBoolean();
        ready = new AtomicBoolean();

        storageById = new ConcurrentHashMap<>();

        entriesByIdPool = new EntriesByIdPool();
        idsToShrink = new ConcurrentLinkedQueue<>();

        removingInProgress = new AtomicBoolean(false);
    }

    private void removeById(Collection<Product> products) {
        for (Product product : products) {
            Integer productId = product.getId();

            EntryById entryById = this.storageById.get(productId);
            boolean isRemoved = entryById.remove(product);
            if (isRemoved && entryById.isEmpty())
                this.storageById.remove(productId);
        }

        log.debug("{} elements was removed from productsById", products.size());
    }

    private void shrinkById() {
        log.debug("Shrinking by id started...");

        List<Product> removed = new LinkedList<>();
        Integer id;
        while ((id = this.idsToShrink.poll()) != null) {
            EntryById entryById = this.storageById.get(id);
            for (int i = entryById.size(); i > limitById; i--) {
                removed.add(entryById.poll());
            }
        }
        log.debug("Shrinking by id finished. {} elements was removed.", removed.size());

        int removedSize = 0;
        for (Product current : removed) {
            if (this.storageTotal.remove(current))
                removedSize++;
        }

        int sizeAfterRemoving = this.size.addAndGet(-removedSize);
        log.debug("StorageById contains {} element after shrinkById", sizeAfterRemoving);
    }

    private void shrinkResult() {
        int size = this.size.get();

        Collection<Product> removed = new LinkedList<>();
        for (int i = size; i > limitTotal; i--)
            removed.add(storageTotal.pollLast());

        this.size.addAndGet(-removed.size());

        removeById(removed);
    }

    private void shrink() {
        shrinkById();
        shrinkResult();
    }

    public boolean addById(Product product) {
        log.debug("Adding product by id {}", product.getId());

        EntryById newEntry = this.entriesByIdPool.get();

        EntryById editingEntry = this.storageById.putIfAbsent(product.getId(), newEntry);
        boolean isNew = editingEntry == null;
        if (isNew)
            editingEntry = newEntry;
        else
            this.entriesByIdPool.add(newEntry);

        boolean added;

        // Could be removed/replaced by another thread
        boolean wasReplaced;
        do {
            added = editingEntry.add(product);

            EntryById currentEntry = this.storageById.putIfAbsent(product.getId(), editingEntry);
            wasReplaced = currentEntry != null && currentEntry != editingEntry;

            if (wasReplaced)
                editingEntry = currentEntry;
        }
        while (wasReplaced);

        return added;
    }

    private void add(Product product) {
        log.debug("Adding to result set {}", product);

        boolean added = storageTotal.add(product);
        if (added) {
            size.incrementAndGet();

            long counter = operationsCounter.incrementAndGet();
            if (counter % operationsToCleanUp == 0L) {
                if (this.removingInProgress.compareAndSet(false, true)) {
                    shrink();
                    this.removingInProgress.set(false);
                }
            }
        }
    }

    public boolean isReady() {
        return this.ready.get();
    }

    public Collection<Product> getResult() {
        log.debug("trying to get result...");

        this.shrink();
        return new LinkedList<>(this.storageTotal);
    }

    public void proceed(Product product) {
        if (this.addById(product))
            this.add(product);

        long proceed = this.proceedTotal.incrementAndGet();
        if (proceed % outputProceedCount == 0)
            log.debug("Totally proceed {}", proceed);

        boolean isLast = (this.stopped.get() && proceed >= this.readTotal.get());
        if (isLast) {
            log.info("Last element was proceed. Totally proceed {}", proceed);
            this.ready.set(true);
        }
    }

    @Override
    public void close() {
        this.storageTotal.clear();
        this.storageById.clear();
        this.idsToShrink.clear();
        this.entriesByIdPool.clear();
    }

    public void addRead(int read) {
        if (!this.stopped.get()) {
            long productRead = this.readTotal.addAndGet(read);
            log.info("Totally {} products read", productRead);
        }
    }

    public void setStopped() {
        log.info("All elements was read, totally read {}", this.readTotal.get());

        this.stopped.set(true);
    }
}