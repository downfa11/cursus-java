package io.cursus.client.saga;

/** Database-neutral transaction-scoped persistence ports for Saga execution. */
public final class SagaContracts {
  private SagaContracts() {}

  public interface InboxStore {
    boolean claim(String consumerName, String eventId) throws Exception;

    void complete(String consumerName, String eventId) throws Exception;

    void fail(String consumerName, String eventId, Exception cause) throws Exception;
  }

  public interface StateStore {
    SagaState loadForUpdate(String sagaType, String sagaId) throws Exception;

    void save(SagaState state) throws Exception;
  }

  public interface CommandOutboxStore {
    void enqueue(SagaCommand command) throws Exception;
  }

  public interface HistoryStore {
    void append(SagaHistoryEvent event) throws Exception;
  }

  public record Stores(
      InboxStore inbox, StateStore state, CommandOutboxStore commandOutbox, HistoryStore history) {}

  @FunctionalInterface
  public interface Operation<T> {
    T apply(Stores stores) throws Exception;
  }

  public interface Transaction {
    <T> T run(Operation<T> operation) throws Exception;
  }
}
