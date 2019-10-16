package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class FileHandleCsv implements FileHandle {
    private final CsvReader csvReader;
    private final Product.ProductCSVCreator productsCreator;
    private final int chunkSize;

    private volatile String filename;
    private volatile Path path;

    private volatile CsvParser csvParser;

    private final AtomicInteger linesRead;

    {
        linesRead = new AtomicInteger();
        linesRead.set(0);
    }

    FileHandleCsv(boolean containsHeaders, char delimiter, int chunkSize) {
        this.csvReader = new CsvReader();
        this.csvReader.setContainsHeader(containsHeaders);
        this.csvReader.setFieldSeparator(delimiter);

        this.productsCreator = containsHeaders ? ProductFromCSVRowByHeaders::new : ProductFromCsvRow::new;

        this.chunkSize = chunkSize;
    }

    private void initParser() {
        try {
            csvParser = csvReader.parse(path, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            log.error("Can't deal with handle of {} file", filename);
            setInactive();
        }
    }

    private void setInactive() {
        try {
            csvParser.close();
        } catch (IOException e) {
            log.warn("failed to properly close file {} {}", filename, e);
        }
        finally {
            csvParser = null;
        }
    }

    @Override
    public void setFileName(String fileName) {
        this.filename = fileName;

        this.path = Paths.get(fileName);
        this.initParser();
    }

    @Override
    public FileHandle reset() {
        this.filename = null;
        this.linesRead.set(0);

        return this;
    }

    @Override
    public String getFileName() {
        return this.filename;
    }

    @Override
    public boolean isActive() {
        return this.csvParser != null;
    }

    @Override
    public int getLines() {
        return this.linesRead.get();
    }

    @Override
    public Optional<List<Product>> getProducts() {
        if (!this.isActive()) {
            return Optional.empty();
        }

        List<Product> products = new LinkedList<>();
        try {
            CsvRow csvRow;
            for (int i = 0; i < this.chunkSize && (csvRow = csvParser.nextRow()) != null; i++)
                products.add(productsCreator.create(csvRow));

            int productsSize = products.size();
            if (productsSize < this.chunkSize) {
                this.setInactive();
            }

            this.linesRead.addAndGet(productsSize);
        } catch (IOException e) {
            log.error("In file with filename {} an error was occupied {}", this.filename, e);
            this.setInactive();
        }

        return Optional.of(products);
    }
}