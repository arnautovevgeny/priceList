package net.ddns.arnautovevgeny.pricelist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class StorageResult implements AutoCloseable {
    private static Logger log = LoggerFactory.getLogger(StorageResult.class);

    private static final int limitTotal = 1000;
    private static final long operationsToCleanUp = 1000;
    private static final long outputProceedCount = 10000;

    private final NavigableSet<Product> storage;
    private final AtomicInteger size;
    private final StampedLock stampedLock;
    private final AtomicLong operationsCounter;

    private final AtomicLong readTotal;
    private final AtomicLong proceedTotal;

    private final AtomicBoolean stopped;
    private final AtomicBoolean ready;

    private StorageById storageById;

    {
        storage = new ConcurrentSkipListSet<>();
        size = new AtomicInteger();
        stampedLock = new StampedLock();
        operationsCounter = new AtomicLong();

        readTotal = new AtomicLong();
        proceedTotal = new AtomicLong();

        stopped = new AtomicBoolean();
        ready = new AtomicBoolean();
    }

    public StorageResult()
    {
        this.storageById = new StorageById();
    }

    // ought to be used only when write lock is obtained
    // reentrant lock isn't supported, because it's StampedLock
    private void shrinkById() {
        log.debug("Shrinking by id started...");

        Collection<Product> removed = this.storageById.shrink();
        log.debug("Shrinking by id finished. {} elements was removed.", removed.size());

        int removedSize = 0;
        for (Product current : removed) {
            if (this.storage.remove(current))
                removedSize++;
        }

        int shouldBeRemoved = removed.size();
        if (removedSize != shouldBeRemoved)
            log.error("{} elements should be removed, {} actually removed", shouldBeRemoved, removedSize);

        int sizeAfterRemoving = this.size.addAndGet(-removedSize);
        log.debug("StorageById contains {} element after shrinkById", sizeAfterRemoving);
    }

    // ought to be used only when write lock is obtained
    // reentrant lock isn't supported, because it's StampedLock
    private void shrinkResult() {
        int size = this.size.get();
        Collection<Product> removed = new LinkedList<>();
        for (int i = size; i > limitTotal; i--)
            removed.add(storage.pollLast());

        if (!this.size.compareAndSet(size, limitTotal))
            log.error("Couldn't set storage size, should be {}, current {} actual size {}", size, this.size.get(), this.storage.size());
        else
            log.debug("Result was shrink to {}, {} elements should be removed from storageById", limitTotal, removed.size());

        storageById.remove(removed);
    }

    private void shrink() {
        shrinkById();
        shrinkResult();
    }

    private void add(Product product) {
        long stamp = stampedLock.readLock();

        log.debug("Adding to result set {}", product);

        try {
            boolean added = storage.add(product);
            if (added) {
                size.incrementAndGet();

                long counter = operationsCounter.incrementAndGet();
                if (counter % operationsToCleanUp == 0L) {
                    // upgrading to exclusive lock
                    long writeStamped = stampedLock.tryConvertToWriteLock(stamp);
                    if (writeStamped != 0L) {
                        stamp = writeStamped;
                    } else {
                        stampedLock.unlockRead(stamp);
                        stamp = stampedLock.writeLock();
                    }

                    log.debug("Shrinking started, size before shrink {}", size.get());
                    shrink();
                    log.debug("Shrinking finished, size after shrink {}", size.get());
                }
            }
        }
        finally {
            stampedLock.unlock(stamp);
        }
    }

    public boolean isReady() {
        return this.ready.get();
    }

    public Collection<Product> getResult() {
        log.debug("trying to get result...");
        long stamp = stampedLock.writeLock();

        try {
            this.shrink();
            return new LinkedList<>(this.storage);
        } finally {
            stampedLock.unlock(stamp);
        }
    }

    public void proceed(Product product) {
        if (this.storageById.add(product))
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

    @Override
    public void close() {
        long stamp = stampedLock.writeLock();
        try {
            this.storage.clear();
            this.storageById.close();
        }
        finally {
            stampedLock.unlock(stamp);
        }
    }
}