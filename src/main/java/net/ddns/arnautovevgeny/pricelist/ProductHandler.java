package net.ddns.arnautovevgeny.pricelist;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
// Consumer
public class ProductHandler implements Runnable {
    private static final AtomicInteger productsTotal = new AtomicInteger();
    private static final int productsToInfoOutput = 10000;

    private TasksBroker tasksBroker;
    public ProductHandler(TasksBroker tasksBroker) {
        this.tasksBroker = tasksBroker;
    }

    @Override
    public void run() {
        do {
            Collection<Product> products;
            while ((products = tasksBroker.getProducts()) != null && !products.isEmpty()) {

                int total = productsTotal.addAndGet(products.size());
                if (total % productsToInfoOutput == 0) {
                    log.debug("Products proceed {}", total);
                }

                for(Product product : products) {
                   tasksBroker.handleProduct(product);
                }
            }
        }
        while (tasksBroker.consumerRequired());

        tasksBroker.consumerStopped();
    }
}
