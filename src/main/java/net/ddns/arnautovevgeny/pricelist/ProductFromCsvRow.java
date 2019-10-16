package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductFromCsvRow extends Product {
    interface ProductCSVRowCreator extends Product.ProductCSVCreator {
        ProductFromCsvRow create(CsvRow row);
    }

    public ProductFromCsvRow(CsvRow row) {
        super(Integer.parseInt(row.getField(0)),
                row.getField(1),
                row.getField(2),
                row.getField(3),
                Float.parseFloat(row.getField(4)));
    }
}