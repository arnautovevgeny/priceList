package net.ddns.arnautovevgeny.pricelist;

public class ProductFromStringArray extends Product {
    public ProductFromStringArray(String[] values) {
        super(Integer.parseInt(values[0]),
                values[1],
                values[2],
                values[3],
                Float.parseFloat(values[4])
                );
    }
}
