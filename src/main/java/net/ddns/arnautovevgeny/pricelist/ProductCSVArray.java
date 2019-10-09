package net.ddns.arnautovevgeny.pricelist;

public class ProductCSVArray extends Product {
    public ProductCSVArray(String[] values) {
        super(Integer.parseInt(values[0]),
                values[1],
                values[2],
                values[3],
                Float.parseFloat(values[4])
                );
    }
}
