package net.ddns.arnautovevgeny.pricelist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class Producer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);
    private TasksBroker tasksBroker;

    public Producer(TasksBroker tasksBroker) {
        this.tasksBroker = tasksBroker;
    }

    @Override
    public void run() {
        do {
            FileHandler fileHandler;
            fileCycle: while ((fileHandler = tasksBroker.getFileHandler()) != null) {
                Optional<List<Product>> products;
                while ((products = fileHandler.getProducts()).isPresent()) {
                    tasksBroker.addProducts(products.get());

                    if (tasksBroker.productQueueIsFull()) {
                        tasksBroker.addFileHandler(fileHandler);
                        break fileCycle;
                    }
                }
                tasksBroker.addFileHandler(fileHandler);
            }
        }
        while (tasksBroker.producerRequired());

        tasksBroker.producerStopped();
    }
}