package net.ddns.arnautovevgeny.pricelist;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
// Producer
public class FileReader implements Runnable {
    private TasksBroker tasksBroker;

    public FileReader(TasksBroker tasksBroker) {
        this.tasksBroker = tasksBroker;
    }

    @Override
    public void run() {
        do {
            FileHandle fileHandle;
            fileCycle: while ((fileHandle = tasksBroker.getFileHandler()) != null) {
                Optional<List<Product>> products;
                while ((products = fileHandle.getProducts()).isPresent()) {
                    tasksBroker.addProducts(products.get());

                    if (tasksBroker.productQueueIsFull()) {
                        tasksBroker.addFileHandler(fileHandle);
                        break fileCycle;
                    }
                }
                tasksBroker.addFileHandler(fileHandle);
            }
        }
        while (tasksBroker.producerRequired());

        tasksBroker.producerStopped();
    }
}