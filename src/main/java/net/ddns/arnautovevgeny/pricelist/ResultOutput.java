package net.ddns.arnautovevgeny.pricelist;

import java.io.IOException;
import java.util.Collection;

public interface ResultOutput {
    void output(Collection<Product> products) throws IOException;
}
