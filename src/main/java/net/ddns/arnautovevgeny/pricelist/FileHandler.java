package net.ddns.arnautovevgeny.pricelist;

import java.util.List;
import java.util.Optional;

public interface FileHandler {
    FileHandler reset();
    void setFileName(String fileName);
    String getFileName();
    boolean isActive();
    int getLines();

    Optional<List<Product>> getProducts();
}
