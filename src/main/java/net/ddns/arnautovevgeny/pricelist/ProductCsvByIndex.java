package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductCsvByIndex extends Product {
    interface ProductCsvByIndexCreator extends Product.ProductCSVCreator {
        ProductCsvByIndex create(CsvRow row);
    }

    public ProductCsvByIndex(CsvRow row) {
        super(Integer.parseInt(row.getField(0)),
                row.getField(1),
                row.getField(2),
                row.getField(3),
                Float.parseFloat(row.getField(4)));
    }
}