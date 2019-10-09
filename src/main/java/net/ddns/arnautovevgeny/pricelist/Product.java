package net.ddns.arnautovevgeny.pricelist;

import de.siegmar.fastcsv.reader.CsvRow;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class Product implements Comparable<Product> {
    interface ProductCSVCreator {
        Product create(CsvRow row);
    }

    private int id;
    private String name;
    private String condition;
    private String state;
    private float price;

    public Product(int id, String name, String condition, String state, float price) {
        this.id = id;
        this.name = name;
        this.condition = condition;
        this.state = state;
        this.price = price;
    }

    public final int getId() {
        return this.id;
    }

    public final String getName() {
        return this.name;
    }

    public final String getCondition() {
        return this.condition;
    }

    public final String getState() {
        return this.state;
    }

    public final float getPrice() {
        return this.price;
    }

    @Override
    public final int compareTo(Product o) {
        int priceCompared = Float.compare(this.price, o.getPrice());
        if (priceCompared != 0)
            return priceCompared;

        int nameCompared = this.name.compareTo(o.getName());
        if (nameCompared != 0)
            return nameCompared;

        int conditionCompared = this.condition.compareTo(o.getCondition());
        if (conditionCompared != 0)
            return conditionCompared;

        int stateCompared = this.state.compareTo(o.getState());
        if (stateCompared != 0)
            return stateCompared;

        int idCompared = Integer.compare(this.id, o.getId());
        if (idCompared != 0)
            return idCompared;

        return 0;
    }
}
