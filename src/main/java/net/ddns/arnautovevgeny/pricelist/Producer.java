package net.ddns.arnautovevgeny.pricelist;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
public class Producer implements Runnable {
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