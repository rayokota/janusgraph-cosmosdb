// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

/*
  The BulkWriter class is an attempt to provide guidance for creating
  a higher level abstraction over the existing low level Java Bulk API
 */
package io.yokota.janusgraph.diskstorage.cosmos.mutation;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class BulkWriter {

  private final Sinks.Many<CosmosItemOperation> bulkInputEmitter = Sinks.many().unicast()
      .onBackpressureBuffer();
  private final int cpuCount = Runtime.getRuntime().availableProcessors();

  // Max items to be buffered to avoid out of memory error
  private final Semaphore semaphore = new Semaphore(1024 * 167 / cpuCount);

  private final Sinks.EmitFailureHandler emitFailureHandler =
      (signalType, emitResult) -> {
        if (emitResult.equals(Sinks.EmitResult.FAIL_NON_SERIALIZED)) {
          log.debug("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
          return true;
        } else {
          log.error("emitFailureHandler - Signal: [{}], Result: [{}]", signalType, emitResult);
          return false;
        }
      };

  private final CosmosAsyncContainer cosmosAsyncContainer;

  public BulkWriter(CosmosAsyncContainer cosmosAsyncContainer) {
    this.cosmosAsyncContainer = cosmosAsyncContainer;
  }

  public void scheduleWrites(CosmosItemOperation cosmosItemOperation) {
    while (!semaphore.tryAcquire()) {
      log.info("Unable to acquire permit");
    }
    log.info("Acquired permit");
    scheduleInternalWrites(cosmosItemOperation);
  }

  private void scheduleInternalWrites(CosmosItemOperation cosmosItemOperation) {
    bulkInputEmitter.emitNext(cosmosItemOperation, emitFailureHandler);
  }

  public Flux<CosmosBulkOperationResponse<?>> execute() {
    return this.execute(null);
  }

  public Flux<CosmosBulkOperationResponse<?>> execute(CosmosBulkExecutionOptions bulkOptions) {
    if (bulkOptions == null) {
      bulkOptions = new CosmosBulkExecutionOptions();
    }
    return cosmosAsyncContainer
        .executeBulkOperations(
            bulkInputEmitter.asFlux(),
            bulkOptions)
        .publishOn(Schedulers.boundedElastic()).map(bulkOperationResponse -> {
          processBulkOperationResponse(
              bulkOperationResponse.getResponse(),
              bulkOperationResponse.getOperation(),
              bulkOperationResponse.getException());

          semaphore.release();
          return bulkOperationResponse;
        });
  }

  private void processBulkOperationResponse(
      CosmosBulkItemResponse itemResponse,
      CosmosItemOperation itemOperation,
      Exception exception) {

    if (exception != null) {
      handleException(itemOperation, exception);
    } else {
      processResponseCode(itemResponse, itemOperation);
    }
  }

  private void processResponseCode(
      CosmosBulkItemResponse itemResponse,
      CosmosItemOperation itemOperation) {

    if (itemResponse.isSuccessStatusCode()) {
      log.info(
          "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] completed successfully " +
              "with a response status code: [{}]",
          itemOperation.getId(),
          itemOperation.getPartitionKeyValue(),
          itemResponse.getStatusCode());
    } else if (shouldRetry(itemResponse.getStatusCode())) {
      log.info(
          "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] will be retried",
          itemOperation.getId(),
          itemOperation.getPartitionKeyValue());
      //re-scheduling
      scheduleWrites(itemOperation);
    } else {
      log.info(
          "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] did not complete successfully "
              +
              "with a response status code: [{}]",
          itemOperation.getId(),
          itemOperation.getPartitionKeyValue(),
          itemResponse.getStatusCode());
    }
  }

  private void handleException(CosmosItemOperation itemOperation, Exception exception) {
    if (!(exception instanceof CosmosException)) {
      log.info(
          "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] encountered an unexpected failure",
          itemOperation.getId(),
          itemOperation.getPartitionKeyValue());
    } else {
      if (shouldRetry(((CosmosException) exception).getStatusCode())) {
        log.info(
            "The operation for Item ID: [{}]  Item PartitionKey Value: [{}] will be retried",
            itemOperation.getId(),
            itemOperation.getPartitionKeyValue());

        //re-scheduling
        scheduleWrites(itemOperation);
      }
    }
  }

  private boolean shouldRetry(int statusCode) {
    return statusCode == HttpConstants.StatusCodes.REQUEST_TIMEOUT ||
        statusCode == HttpConstants.StatusCodes.TOO_MANY_REQUESTS;
  }
}
