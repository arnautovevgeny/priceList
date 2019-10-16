package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductCSVRowWithHeaders extends Product {
    interface ProductCSVRowHeadersCreator extends Product.ProductCSVCreator {
        ProductCSVRowWithHeaders create(CsvRow row);
    }

    public ProductCSVRowWithHeaders(CsvRow row) {
        super(Integer.parseInt(row.getField("product ID")),
                row.getField("Name"),
                row.getField("Condition"),
                row.getField("State"),
                Float.parseFloat(row.getField("Price")));
    }
}