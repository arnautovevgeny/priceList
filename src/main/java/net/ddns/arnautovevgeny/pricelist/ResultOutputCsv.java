package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.writer.CsvWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;

public class ResultOutputCsv implements ResultOutput {
    private final Path filePath;
    private boolean headersIncluded;
    private char separator;

    public ResultOutputCsv(Path filePath, boolean headersIncluded, char separator) {
        this.filePath = filePath;
        this.headersIncluded = headersIncluded;
        this.separator = separator;
    }

    @Override
    public void output(Collection<Product> products) throws IOException {
        CsvWriter csvWriter = new CsvWriter();
        csvWriter.setFieldSeparator(this.separator);

        Collection<String[]> data = new LinkedList<>();
        if (this.headersIncluded)
            data.add(new String[]{"product ID", "Name", "Condition", "State", "Price"});

        for(Product product : products)
            data.add(new String[]{Integer.toString(product.getId()), product.getName(), product.getCondition(), product.getState(), Float.toString(product.getPrice())});

        csvWriter.write(this.filePath, StandardCharsets.UTF_8, data);
    }
}
