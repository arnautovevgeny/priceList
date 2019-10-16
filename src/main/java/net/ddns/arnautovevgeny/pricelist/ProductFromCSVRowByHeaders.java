package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductFromCSVRowByHeaders extends Product {
    interface ProductCSVRowByHeadersCreator extends Product.ProductCSVCreator {
        ProductFromCSVRowByHeaders create(CsvRow row);
    }

    public ProductFromCSVRowByHeaders(CsvRow row) {
        super(Integer.parseInt(row.getField("product ID")),
                row.getField("Name"),
                row.getField("Condition"),
                row.getField("State"),
                Float.parseFloat(row.getField("Price")));
    }
}