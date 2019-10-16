package net.ddns.arnautovevgeny.pricelist;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.stream.Stream;

@Slf4j
public class PriceList {
    @Getter
    private Collection<Product> products;
    private Path path;
    private boolean includeHeaders;
    private char delimiter;

    public PriceList(Path path, boolean includeHeaders, char delimiter) {
        this.path = path;
        this.includeHeaders = includeHeaders;
        this.delimiter = delimiter;
    }

    public static void main(String[] csvFiles) throws InterruptedException, IOException {
        if (csvFiles.length != 0) {
            Path path = Paths.get("result.csv");

            PriceList priceList = new PriceList(path, false, ',');
            priceList.processViaStreamAPI(csvFiles);
            log.info("Result contains {} elements", priceList.getSize());

            priceList.output();
            log.info("Finished");
        }
        else
            System.out.println("No files to proceed was specified");
    }

    public void processViaProducerConsumer(String[] csvFiles, boolean loadBalancer) throws InterruptedException {
        try (TasksBroker tasksBroker = new TasksBroker(csvFiles, this.includeHeaders, this.delimiter)) {
            tasksBroker.start(loadBalancer);

            tasksBroker.awaitsTermination();

            this.products = tasksBroker.getResult();
        }
    }

    public void processViaStreamAPI(String[] csvFiles) {
        String strDelimiter = "" + this.delimiter;
        try (ResultStorage resultStorage = new ResultStorage()) {

            Stream<Path> pathStream = Stream.of(csvFiles).parallel().map(file -> Paths.get(file));
            Stream<String> lines = pathStream.flatMap(path -> {
                try {
                    Stream<String> linesOfFile = Files.lines(path);
                    if (this.includeHeaders) {
                        linesOfFile = linesOfFile.skip(1);
                    }

                    return linesOfFile;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return Stream.empty();
            });

            lines.map(line -> new ProductFromStringArray(line.split(strDelimiter))).forEach(resultStorage::proceed);
            this.products = resultStorage.getResult();
        }
    }

    public void output() throws IOException {
        ResultOutput resultOutput = new ResultOutputCsv(path, includeHeaders, delimiter);
        resultOutput.output(this.products);

        log.info("Result was outputted to {}", path.toAbsolutePath());
    }

    public int getSize() {
        return this.products.size();
    }
}
