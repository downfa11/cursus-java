package io.cursus.client.saga;

import java.util.List;
import java.util.Map;

/** A Saga's event handlers, with no database or broker dependency. */
public record SagaDefinition(String sagaType, Map<String, Handler> handlers) {
  @FunctionalInterface public interface Handler { List<SagaCommand> handle(SagaState state, SagaEventEnvelope event) throws Exception; }
}
