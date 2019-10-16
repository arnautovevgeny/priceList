package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductCsvByHeader extends Product {
    interface ProductCsvByHeaderCreator extends Product.ProductCSVCreator {
        ProductCsvByHeader create(CsvRow row);
    }

    public ProductCsvByHeader(CsvRow row) {
        super(Integer.parseInt(row.getField("product ID")),
                row.getField("Name"),
                row.getField("Condition"),
                row.getField("State"),
                Float.parseFloat(row.getField("Price")));
    }
}