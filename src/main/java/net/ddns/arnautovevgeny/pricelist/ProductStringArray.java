package net.ddns.arnautovevgeny.pricelist;

public class ProductStringArray extends Product {
    public ProductStringArray(String[] values) {
        super(Integer.parseInt(values[0]),
                values[1],
                values[2],
                values[3],
                Float.parseFloat(values[4])
                );
    }
}
