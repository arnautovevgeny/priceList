package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;

public class ProductCSVRowSimple extends Product {
    interface ProductCSVRowSimpleCreator extends Product.ProductCSVCreator {
        ProductCSVRowSimple create(CsvRow row);
    }

    public ProductCSVRowSimple(CsvRow row) {
        super(Integer.parseInt(row.getField(0)),
                row.getField(1),
                row.getField(2),
                row.getField(3),
                Float.parseFloat(row.getField(4)));
    }
}