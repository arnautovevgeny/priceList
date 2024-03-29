package net.ddns.arnautovevgeny.pricelist;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

@Slf4j
public class TasksBroker implements AutoCloseable {
    private static final int maxProductsInQueue = 100000;
    private static final int maxProductsToProduce = maxProductsInQueue / 100;
    private static final int maxProductsToConsume = maxProductsToProduce / 2;
    private static final int stepSizeToProduce = maxProductsToProduce / 10;
    private static final int stepSizeToConsume = maxProductsToConsume / 10;
    private static final int initialChunkToProduce = 100;
    private static final int initialChunkToConsume = 100;

    private static final int processorsCounter = Runtime.getRuntime().availableProcessors();
    private static final int producersLimit = processorsCounter - 1;
    private static final int consumersLimit = processorsCounter - 1;

    private static final IntUnaryOperator decreaseTillOne = oldValue -> {
        int newValue = oldValue - 1;
        return newValue > 0 ? newValue : oldValue;
    };

    private static final IntUnaryOperator increaseProducersChunk = oldValue -> {
        int newValue = oldValue + stepSizeToProduce;
        return (Math.min(newValue, maxProductsToProduce));
    };

    private static final IntUnaryOperator increaseConsumersChunk = oldValue -> {
        int newValue = oldValue + stepSizeToConsume;
        return (Math.min(newValue, maxProductsToConsume));
    };

    private static final int warningsToOutput = 100000;

    private final ExecutorService executorService;
    private final AtomicBoolean shutdownNeeded;

    private final FileReader fileReader;
    private final ProductHandler productHandler;

    private final AtomicInteger producersRequired;
    private final AtomicInteger consumersRequired;

    private final AtomicInteger producersActual;
    private final AtomicInteger consumersActual;

    private final boolean containsHeaders;
    private final char delimiter;
    private final Queue<String> filesList;
    private final AtomicInteger filesToProceedCounter;

    private final Queue<FileHandle> activeHandlesPool;
    private final Queue<FileHandle> inactiveHandlesPool;
    private final List<Queue<FileHandle>> handlesPoolList;

    private final BlockingQueue<Product> productsList;
    private final AtomicInteger productsCounter;
    private final AtomicInteger queueFullWarnings;

    private final ResultStorage resultStorage;

    private boolean loadBalancer;
    private AtomicInteger chunkSizeToProduce;
    private AtomicInteger chunkSizeToConsume;

    {
        fileReader = new FileReader(this);
        productHandler = new ProductHandler(this);

        producersRequired = new AtomicInteger(processorsCounter / 2);
        consumersRequired = new AtomicInteger(processorsCounter / 2);

        chunkSizeToProduce = new AtomicInteger(initialChunkToProduce);
        chunkSizeToConsume = new AtomicInteger(initialChunkToConsume);

        producersActual = new AtomicInteger();
        consumersActual = new AtomicInteger();

        filesList = new ConcurrentLinkedQueue<>();
        filesToProceedCounter = new AtomicInteger();

        activeHandlesPool = new ConcurrentLinkedQueue<>();
        inactiveHandlesPool = new ConcurrentLinkedQueue<>();

        productsList = new LinkedBlockingQueue<>(maxProductsInQueue);

        productsCounter = new AtomicInteger();
        queueFullWarnings = new AtomicInteger();

        executorService = Executors.newFixedThreadPool(processorsCounter);

        shutdownNeeded = new AtomicBoolean();

        handlesPoolList = List.of(activeHandlesPool, inactiveHandlesPool);

        resultStorage = new ResultStorage();
    }

    public TasksBroker(String[] csvFiles, boolean containsHeaders, char delimiter) {
        this.filesList.addAll(Arrays.asList(csvFiles));
        this.containsHeaders = containsHeaders;
        this.delimiter = delimiter;

        int filesCount = this.filesToProceedCounter.addAndGet(csvFiles.length);
        log.info("Ready to proceed {} files", filesCount);
    }

    public void start(boolean loadBalancer) {
        this.loadBalancer = loadBalancer;

        int producersCount = producersRequired.get();
        int consumersCount = consumersRequired.get();

        for (int i = producersCount; i > 0; i--) {
            executorService.submit(fileReader);
            producersActual.incrementAndGet();
        }

        for (int i = consumersCount; i > 0; i--) {
            executorService.submit(productHandler);
            consumersActual.incrementAndGet();
        }

        log.info("{} producers and {} consumers was started", producersCount, consumersCount);
    }

    private boolean producingCompleted() {
        return filesList.isEmpty() && filesToProceedCounter.get() <= 0;
    }

    private boolean consumingCompleted() {
        return producingCompleted() && productQueueIsEmpty();
    }

    private void updateProcessingState() {
        if (producingCompleted() && consumingCompleted() && isReady()) {
            if (shutdownNeeded.compareAndSet(false, true)) {
                log.info("Shutting down the executorService");
                executorService.shutdown();
            }
        }
    }

    private void increaseProducing() {
        if (!producingCompleted()) {
            boolean lastConsumer = consumersRequired.updateAndGet(decreaseTillOne) == 1;
            if (lastConsumer) {
                int chunkSize = chunkSizeToProduce.updateAndGet(increaseProducersChunk);
                log.debug("Chunk size to produce {}", chunkSize);
            }

            int producersRequired = this.producersRequired.incrementAndGet();
            log.debug("substituteByConsumer, consumersRequired {}, limit {}", consumersRequired, consumersLimit);
            if (producersRequired <= producersLimit) {
                int actual = producersActual.incrementAndGet();
                log.debug("There are {} producers actual", actual);

                executorService.submit(fileReader);
            }
            else
                producersRequired = this.producersRequired.decrementAndGet();

            log.debug("substituteByProducer, producersRequired {}", producersRequired);
        }
    }

    private void increaseConsuming() {
        if (!consumingCompleted()) {
            boolean lastProducer = producersRequired.updateAndGet(decreaseTillOne) == 1;
            if (lastProducer) {
                int chunkSize = chunkSizeToConsume.updateAndGet(increaseConsumersChunk);
                log.debug("Chunk size to consume {}", chunkSize);
            }

            int consumersCount = consumersRequired.incrementAndGet();
            log.debug("substituteByConsumer, consumersRequired {}, limit {}", consumersCount, consumersLimit);
            if (consumersCount <= consumersLimit) {
                int actual = consumersActual.incrementAndGet();
                log.debug("There are {} consumers actual", actual);

                executorService.submit(productHandler);
            }
            else
                consumersCount = this.consumersRequired.decrementAndGet();

            log.debug("substituteByConsumer, consumersRequired {}", consumersCount);
        }
    }

    private boolean isReady() {
        return this.resultStorage.isReady();
    }

    boolean productQueueIsFull() {
        return this.productsCounter.get() >= maxProductsInQueue;
    }

    private boolean productQueueIsEmpty() {
        return this.productsList.isEmpty();
    }

    void addProducts(Collection<Product> products) {
        for (Product product : products) {
            if (!productsList.offer(product)) {
                int warningsCount = queueFullWarnings.incrementAndGet();
                if (warningsCount % warningsToOutput == 0)
                    log.warn("Couldn't offer a product, queue is full. Currently there were {} warnings", warningsCount);

                if (loadBalancer)
                    increaseConsuming();

                try {
                    productsList.put(product);
                } catch (InterruptedException e) {
                    log.error("Thread was interrupted and couldn't put product into a queue", e);
                }
            }
        }

        this.productsCounter.addAndGet(products.size());
    }

    Collection<Product> getProducts() {
        List<Product> productsChunk = new LinkedList<>();
        productsList.drainTo(productsChunk, chunkSizeToConsume.get());

        int chunkSize = productsChunk.size();
        this.productsCounter.addAndGet(-chunkSize);

        if (loadBalancer && chunkSize <= 0)
            this.increaseProducing();

        return productsChunk;
    }

    void addFileHandler(FileHandle fileHandle) {
        if (fileHandle.isActive())
            this.activeHandlesPool.offer(fileHandle);
        else
        {
            int count = this.filesToProceedCounter.decrementAndGet();
            log.info("There are {} files left to proceed, last processed {}", count, fileHandle.getFileName());

            this.resultStorage.addRead(fileHandle.getLines());
            if (count == 0)
                this.resultStorage.setStopped();
            else
                this.inactiveHandlesPool.offer(fileHandle.reset());
        }
    }

    FileHandle getFileHandler() {
        FileHandle handle = null;

        for (var fileHandlers : this.handlesPoolList) {
            if ((handle = fileHandlers.poll()) != null) {
                break;
            }
        }

        if (handle == null || !handle.isActive()) {
            String fileName = this.filesList.poll();
            if (fileName == null)
                handle = null;
            else
            {
                if (handle == null)
                    handle = new FileHandleCsv(containsHeaders, delimiter, chunkSizeToProduce.get());

                handle.setFileName(fileName);
            }
        }

        return handle;
    }

    boolean producerRequired() {
        return !this.producingCompleted() && this.producersActual.get() <= this.producersRequired.get();
    }

    void producerStopped() {
        int actual = this.producersActual.decrementAndGet();
        log.debug("Currently {} producers", actual);

        updateProcessingState();
    }

    boolean consumerRequired() {
        return !this.consumingCompleted() && this.consumersActual.get() <= this.consumersRequired.get();
    }

    void consumerStopped() {
        int actual = this.consumersActual.decrementAndGet();
        log.debug("Currently {} consumers", actual);

        updateProcessingState();
    }

    void handleProduct(Product product) {
        this.resultStorage.handle(product);
    }

    public void awaitsTermination() throws InterruptedException {
        this.executorService.awaitTermination(365, TimeUnit.DAYS);
    }

    public Collection<Product> getResult() {
        if (this.isReady())
            return this.resultStorage.getResult();
        else {
            log.error("Trying to get result while not ready");
            return new LinkedList<>();
        }
    }

    @Override
    public void close() {
        this.resultStorage.close();
    }
}