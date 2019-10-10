package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.writer.CsvWriter;
import lombok.Getter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AutomationTest {
    private static class RandomProductGenerator implements Supplier<Product> {
        static final int minPriceInCentsDefault = 1;
        static final int maxPriceInCentsDefault = 9999999;

        private final Random random = new Random();
        private final Queue<Integer> idsToReuse = new ConcurrentLinkedQueue<>();

        private final int minimalPriceInCents;
        private final int maximalPriceInCents;
        private final AtomicInteger numberSequence;

        RandomProductGenerator(int minimalId, int minimalPriceInCents, int maximalPriceInCents) {
            if (minimalId <= 0 || minimalPriceInCents <= 0 || maximalPriceInCents <= 0 || minimalPriceInCents > maximalPriceInCents) {
                log.error("minimalId {}, minimalPriceInCents {}, maximalPriceInCents {}", minimalId, minimalPriceInCents, maximalPriceInCents);
                throw new IllegalArgumentException();
            }

            this.numberSequence = new AtomicInteger(minimalId);
            this.minimalPriceInCents = minimalPriceInCents;
            this.maximalPriceInCents = maximalPriceInCents;
        }

        RandomProductGenerator(int minimalId, int minimalPriceInCents) {
            this(minimalId, minimalPriceInCents, maxPriceInCentsDefault);
        }

        RandomProductGenerator(int minimalId) {
            this(minimalId, minPriceInCentsDefault);
        }

        RandomProductGenerator() {
            this(1);
        }

        @Override
        public Product get() {
            Integer id = null;
            boolean newIdNeeded = this.random.nextBoolean();
            if (!newIdNeeded)
                id = this.idsToReuse.poll();

            if (id == null)
                id = this.numberSequence.getAndIncrement();

            this.idsToReuse.offer(id);

            float randomPrice = this.minimalPriceInCents + this.random.nextInt(maximalPriceInCents - minimalPriceInCents + 1);
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

        void addLine(Path path, String[] line) {
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

    private final static FileNameGenerator fileNameGenerator = new FileNameGenerator(System.getProperty("user.home") + "\\generated");
    private final static CsvWriter csvWriter = new CsvWriter();

    private static final int limitById = 20;
    private static final int limitTotal = 1000;
    private static final int productsToGenerateLimit = 1000000;
    private static final int filesToCreate = 1000;

    private static final boolean includeHeaders = false;
    private static final char delimiter = ',';

    private static final int expectedProductsMaxPriceInCents = 99999;
    private static Collection<Product> expectedProducts;

    private static String[] csvFiles;
    private static Path[] csvPaths;

    private static void calcExpectedResult() {
        RandomProductGenerator expectedProductsGenerator = new RandomProductGenerator(1, RandomProductGenerator.minPriceInCentsDefault, expectedProductsMaxPriceInCents);

        Map<Integer, NavigableSet<Product>> groupedByID = Stream.generate(expectedProductsGenerator).parallel().limit(10 * limitTotal * limitById).
                collect(Collectors.groupingByConcurrent(Product::getId, Collectors.toCollection(ConcurrentSkipListSet::new)));

        var expectedBeforeShrink = groupedByID.entrySet().parallelStream().map(e -> {
                    NavigableSet<Product> products = e.getValue();

                    int size = products.size();
                    for (int i = size; i > limitById; i--)
                        products.pollLast();

                    return products;
                }
        ).flatMap(Collection::stream).collect(Collectors.toCollection(ConcurrentSkipListSet::new));

        expectedProducts = expectedBeforeShrink.stream().limit(limitTotal).collect(Collectors.toCollection(ConcurrentSkipListSet::new));
    }

    private static void createCsvFiles(Stream<Product> generatedProducts, Path[] pathsArray) {
        int pathSize = pathsArray.length;
        log.info("pathsArray was generated with size {}", pathSize);

        Random random = new Random();
        LinesByPath linesByPath = new LinesByPath();

        generatedProducts.parallel().map(
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

    private static Stream<Product> generateProducts()
    {
        calcExpectedResult();

        int expectedResultSize = expectedProducts.size();
        log.info("Expected result is obtained. It contains {} elements", expectedResultSize);

        int maxExpectedId = (expectedProducts.stream().mapToInt(Product::getId).max().orElse(0));
        int maxExpectedPriceInCents = expectedProductsMaxPriceInCents + 1;

        int minimalAdditionId = maxExpectedId + 1;
        int minimalAdditionPriceInCents = maxExpectedPriceInCents + 1;

        log.info("minimal addition id {}, minimal addition price in cents {}", minimalAdditionId, minimalAdditionPriceInCents);

        RandomProductGenerator additionalGenerator = new RandomProductGenerator(minimalAdditionId, minimalAdditionPriceInCents);

        int additionLimit = productsToGenerateLimit - expectedResultSize;

        Stream<Product> additionStream = Stream.generate(additionalGenerator).limit(additionLimit);

        return Stream.concat(additionStream, expectedProducts.stream());
    }

    @BeforeAll
    public static void createFiles() {
        var generatedProducts = generateProducts();

        csvFiles = Stream.generate(fileNameGenerator).limit(filesToCreate).toArray(String[]::new);

        csvPaths = Stream.of(csvFiles).map(filename -> Paths.get(filename)).toArray(Path[]::new);

        createCsvFiles(generatedProducts, csvPaths);

        log.info("{} files was generated in {}", filesToCreate, Paths.get(fileNameGenerator.getTemplateName()).getParent().toAbsolutePath());
    }

    @Test
    public void testViaStream() throws IOException {
        PriceList priceList = new PriceList(Paths.get("resultStream.csv"), includeHeaders, delimiter);
        priceList.processViaStreamAPI(this.csvFiles);

        Collection<Product> actual = priceList.getProducts();
        priceList.output();

        assertEquals(new LinkedList<>(this.expectedProducts), actual);
    }

    private void producerConsumer(boolean loadBalancer) throws IOException, InterruptedException {
        PriceList priceList = new PriceList(Paths.get("resultProducerConsumer.csv"), includeHeaders, delimiter);
        priceList.processViaProducerConsumer(this.csvFiles, loadBalancer);

        Collection<Product> actual = priceList.getProducts();
        priceList.output();

        assertEquals(new LinkedList<>(this.expectedProducts), actual);
    }

    @Test
    public void testViaProducerConsumer() throws IOException, InterruptedException {
        this.producerConsumer(false);
    }

    @Test
    public void testViaProducerConsumerLoadBalancer() throws IOException, InterruptedException {
        this.producerConsumer(true);
    }

    @AfterAll
    public static void outputExpectedResult() throws IOException {
        ResultOutput resultOutput = new ResultOutputCsv(Paths.get("expected.csv"), includeHeaders, delimiter);
        resultOutput.output(expectedProducts);
    }

    @AfterAll
    public static void removeGeneratedFiles() throws IOException {
        for (Path path : csvPaths)
            Files.deleteIfExists(path);
    }
}
