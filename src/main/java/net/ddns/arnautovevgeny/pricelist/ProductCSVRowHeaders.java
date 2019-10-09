package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductCSVRowHeaders extends Product {
    interface ProductCSVRowHeadersCreator extends Product.ProductCSVCreator {
        ProductCSVRowHeaders create(CsvRow row);
    }

    public ProductCSVRowHeaders(CsvRow row) {
        super(Integer.parseInt(row.getField("product ID")),
                row.getField("Name"),
                row.getField("Condition"),
                row.getField("State"),
                Float.parseFloat(row.getField("Price")));
    }
}