package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.writer.CsvWriter;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class AutomationTest {
    private static class RandomProductGenerator implements Supplier<Product> {
        private static final Random random = new Random();
        private static final AtomicInteger numberSequence = new AtomicInteger();
        private static final Queue<Integer> idsToReuse = new ConcurrentLinkedQueue<>();
        private static final int maximalPriceInCents = 9999999;

        @Override
        public Product get() {
            Integer id;
            boolean newIdNeeded = random.nextBoolean();
            if (newIdNeeded)
                id = numberSequence.incrementAndGet();
            else {
                id = idsToReuse.poll();
                if (id == null) {
                    id = numberSequence.incrementAndGet();
                }
            }

            idsToReuse.offer(id);
            float randomPrice = (random.nextInt() & Integer.MAX_VALUE) % maximalPriceInCents;
            randomPrice /= 100;

            return new Product(id, "product " + id, "new", "ok", randomPrice);
        }
    }

    private static class FileNameGenerator implements Supplier<String> {
        @Getter
        private final String templateName;
        private final AtomicInteger numberSequence = new AtomicInteger();

        FileNameGenerator(String templateName) {
            this.templateName = templateName;
        }

        @Override
        public String get() {
            return this.templateName + this.numberSequence.incrementAndGet() + ".csv";
        }
    }

    private static class LinesByPath {
        @Getter
        private Map<Path, Queue<String[]>> storage = new ConcurrentHashMap<>();

        private Queue<Queue<String[]>> emptyLinesPool = new ConcurrentLinkedQueue<>();

        private Queue<String[]> getFromPool() {
            Queue<String[]> entry = emptyLinesPool.poll();
            if (entry == null) {
                entry = new ConcurrentLinkedQueue<>();
                if (includeHeaders)
                    entry.offer(new String[]{"product ID", "Name", "Condition", "State", "Price"});
            }

            return entry;
        }

        private void addToPool(Queue<String[]> entry) {
            emptyLinesPool.offer(entry);
        }

        public void addLine(Path path, String[] line) {
            Queue<String[]> newEntry = getFromPool();

            Queue<String[]> entry = storage.putIfAbsent(path, newEntry);
            boolean isNew = entry == null;
            if (isNew)
                entry = newEntry;
            else
                addToPool(newEntry);

            entry.offer(line);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(AutomationTest.class);

    private final RandomProductGenerator randomProductGenerator = new RandomProductGenerator();
    private final FileNameGenerator fileNameGenerator = new FileNameGenerator(System.getProperty("user.home") + "\\generated");
    private final CsvWriter csvWriter = new CsvWriter();

    private static final int limitById = 20;
    private static final int limitTotal = 1000;
    private static final int productsToGenerate = 100000;
    private static final int filesToCreate = 100;

    private static final boolean includeHeaders = false;
    private static final char delimiter = ',';

    private Collection<Product> expectedProducts;
    private String[] csvFiles;
    private Path[] csvPaths;

    private static Collection<Product> getExpectedResult(Map<Integer, NavigableSet<Product>> groupedByID) {
        var expectedBeforeShrink = groupedByID.entrySet().parallelStream().map(e -> {
                    NavigableSet<Product> products = e.getValue();

                    int size = products.size();
                    for (int i = size; i > limitById; i--)
                        products.pollLast();

                    return products;
                }
        ).flatMap(Collection::stream).collect(Collectors.toCollection(ConcurrentSkipListSet::new));

        var expected = expectedBeforeShrink.stream().limit(limitTotal).collect(Collectors.toCollection(ConcurrentSkipListSet::new));
        return new LinkedList<>(expected);
    }

    private void createCsvFiles(Collection<Product> generatedProducts, Path[] pathsArray) {
        int pathSize = pathsArray.length;
        log.info("pathsArray was generated with size {}", pathSize);

        Random random = new Random();
        LinesByPath linesByPath = new LinesByPath();

        generatedProducts.parallelStream().map(
                product -> new String[]{Integer.toString(product.getId()), product.getName(), product.getCondition(), product.getState(), Float.toString(product.getPrice())}
        ).forEach(line -> {
                    Path path = pathsArray[(random.nextInt() & Integer.MAX_VALUE) % pathSize];
                    linesByPath.addLine(path, line);
                }
        );

        log.info("Lines for files prepared in a parallel manner");

        csvWriter.setFieldSeparator(delimiter);

        var linesStorage = linesByPath.getStorage();
        linesStorage.entrySet().parallelStream().forEach(e -> {
            try {
                csvWriter.write(e.getKey(), StandardCharsets.UTF_8, e.getValue());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }

    @Before
    public void createFiles() {
        Collection<Product> generatedProducts = Stream.generate(randomProductGenerator).parallel().
                limit(productsToGenerate).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

        log.info("{} products was generated in a parallel manner", generatedProducts.size());

        Map<Integer, NavigableSet<Product>> groupedByID = generatedProducts.parallelStream().
                collect(Collectors.groupingByConcurrent(Product::getId, Collectors.toCollection(ConcurrentSkipListSet::new)));

        log.info("Products was grouped onto a ConcurrentHashMap<K,V> by ProductId as a K");

        this.expectedProducts = getExpectedResult(groupedByID);
        log.info("Expected result is obtained. It contains {} elements", this.expectedProducts.size());

        this.csvFiles = Stream.generate(this.fileNameGenerator).limit(filesToCreate).toArray(String[]::new);

        this.csvPaths = Stream.of(this.csvFiles).map(filename -> Paths.get(filename)).toArray(Path[]::new);

        this.createCsvFiles(generatedProducts, this.csvPaths);

        log.info("{} files was generated in {}", filesToCreate, Paths.get(this.fileNameGenerator.getTemplateName()).getParent().toAbsolutePath());
    }

    @Test
    public void testViaStream() throws IOException {
        PriceList priceList = new PriceList(Paths.get("resultStream.csv"), includeHeaders, delimiter);
        priceList.processViaStreamAPI(this.csvFiles);

        Collection<Product> actual = priceList.getProducts();
        priceList.output();

        assertEquals(this.expectedProducts, actual);
    }

    private void producerConsumer(boolean loadBalancer) throws IOException, InterruptedException {
        PriceList priceList = new PriceList(Paths.get("resultProducerConsumer.csv"), includeHeaders, delimiter);
        priceList.processViaProducerConsumer(this.csvFiles, loadBalancer);

        Collection<Product> actual = priceList.getProducts();
        priceList.output();

        assertEquals(this.expectedProducts, actual);
    }

    @Test
    public void testViaProducerConsumer() throws IOException, InterruptedException {
        this.producerConsumer(false);
    }

    @Test
    public void testViaProducerConsumerLoadBalancer() throws IOException, InterruptedException {
        this.producerConsumer(true);
    }

    @After
    public void outputExpectedResult() throws IOException {
        ResultOutput resultOutput = new ResultOutputCsv(Paths.get("expected.csv"), includeHeaders, delimiter);
        resultOutput.output(this.expectedProducts);
    }

    @After
    public void removeGeneratedFiles() throws IOException {
        for (Path path : this.csvPaths)
            Files.deleteIfExists(path);
    }
}
