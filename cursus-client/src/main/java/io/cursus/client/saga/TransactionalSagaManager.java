package io.cursus.client.saga;

import io.cursus.client.saga.SagaContracts.Stores;
import io.cursus.client.saga.SagaContracts.Transaction;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Opt-in manager that persists inbox, state, command outbox, and history atomically. */
public final class TransactionalSagaManager {
  public static final String RUN_STARTED = "run.started";
  public static final String RUN_WAITING = "run.waiting";
  public static final String STEP_STARTED = "step.started";
  public static final String STEP_COMPLETED = "step.completed";
  public static final String STEP_FAILED = "step.failed";
  public static final String COMMAND_ENQUEUED = "command.enqueued";
  public static final String COMMAND_PUBLISHED = "command.published";
  public static final String COMMAND_SUCCEEDED = "command.succeeded";
  public static final String COMMAND_FAILED = "command.failed";
  public static final String COMPENSATION_STARTED = "compensation.started";
  public static final String COMPENSATION_COMPLETED = "compensation.completed";
  public static final String COMPENSATION_FAILED = "compensation.failed";
  public static final String RUN_COMPLETED = "run.completed";
  public static final String RUN_FAILED = "run.failed";
  public static final String RUN_COMPENSATED = "run.compensated";
  public static final String OUTCOME_SUCCEEDED = "SUCCEEDED";
  public static final String OUTCOME_COMPENSATED = "COMPENSATED";
  public static final String OUTCOME_FAILED = "FAILED";

  private final SagaDefinition definition;
  private final Transaction transaction;
  private final SagaHistoryOptions options;

  public TransactionalSagaManager(SagaDefinition definition, Transaction transaction, SagaHistoryOptions options) {
    if (definition == null || definition.sagaType() == null || definition.sagaType().isBlank()
        || definition.handlers() == null || definition.handlers().isEmpty()) {
      throw new IllegalArgumentException("Saga definition requires type and handlers");
    }
    this.definition = definition;
    this.transaction = transaction;
    this.options = options;
  }

  public void handle(SagaEventEnvelope event) throws Exception {
    if (blank(event.eventId()) || blank(event.eventType())) throw new IllegalArgumentException("event identity is required");
    String key = associationKey(event);
    try {
      transaction.run(stores -> { handle(stores, key, event); return null; });
    } catch (Exception cause) {
      recordRolledBackFailure(key, event, cause);
      throw cause;
    }
  }

  private void handle(Stores stores, String key, SagaEventEnvelope event) throws Exception {
    if (!stores.inbox().claim(definition.sagaType(), event.eventId())) return;
    SagaState state = stores.state().loadForUpdate(definition.sagaType(), key);
    if (state == null) {
      state = new SagaState(key, definition.sagaType(), key);
      state.setCorrelationId(empty(event.correlationId()));
      record(stores, state, event, RUN_STARTED, "", null, null, "");
    }
    SagaDefinition.Handler handler = definition.handlers().get(event.eventType());
    if (handler == null) {
      stores.state().save(state);
      stores.inbox().complete(definition.sagaType(), event.eventId());
      return;
    }
    int attempt = state.getRetryCount() + 1;
    List<SagaCommand> commands = handler.handle(state, event);
    String stepId = state.getStepId().isEmpty() ? event.eventType() : state.getStepId();
    record(stores, state, event, STEP_STARTED, stepId, attempt, null, "");
    for (int index = 0; index < commands.size(); index++) {
      SagaCommand command = commands.get(index);
      String effectId = command.getEffectId().isEmpty() ? event.eventId() + ":" + index : command.getEffectId();
      SagaState.EffectState effect = state.getEffects().get(effectId);
      if (effect != null && !SagaState.EFFECT_FAILED.equals(effect.getStatus())) continue;
      command.effectId(effectId).sagaType(state.getSagaType()).sagaId(state.getSagaId())
          .correlationId(command.getCorrelationId().isEmpty() ? state.getCorrelationId() : command.getCorrelationId())
          .causationId(command.getCausationId().isEmpty() ? event.eventId() : command.getCausationId());
      command.ensureCommandId();
      if (effect == null) effect = new SagaState.EffectState(effectId, command.getType());
      effect.setStepId(command.getType());
      effect.setAttempts(effect.getAttempts() + 1);
      effect.setStatus(SagaState.PENDING);
      effect.setCommandId(command.getCommandId());
      effect.setLastError("");
      effect.setUpdatedAt(Instant.now());
      state.getEffects().put(effectId, effect);
      stores.commandOutbox().enqueue(command);
      record(stores, state, event, COMMAND_ENQUEUED, command.getType(), effect.getAttempts(), command, "");
    }
    record(stores, state, event, STEP_COMPLETED, stepId, attempt, null, "");
    if (SagaState.WAITING.equals(state.getStatus())) record(stores, state, event, RUN_WAITING, stepId, attempt, null, "");
    if (SagaState.COMPLETED.equals(state.getStatus())) {
      state.setOutcome(OUTCOME_SUCCEEDED);
      record(stores, state, event, RUN_COMPLETED, "", null, null, "");
    }
    if (SagaState.FAILED.equals(state.getStatus())) {
      state.setOutcome(OUTCOME_FAILED);
      record(stores, state, event, RUN_FAILED, "", null, null, state.getLastError());
    }
    state.setUpdatedAt(Instant.now());
    stores.state().save(state);
    stores.inbox().complete(definition.sagaType(), event.eventId());
  }

  public void recordCommandPublished(String key, String effectId) throws Exception {
    transaction.run(stores -> {
      SagaState state = state(stores, key);
      SagaState.EffectState effect = effect(state, effectId);
      if (!effect.isPublished() && SagaState.PENDING.equals(effect.getStatus())) {
        SagaCommand command = command(state, effect);
        record(stores, state, emptyEvent(), COMMAND_PUBLISHED, effect.getStepId(), effect.getAttempts(), command, "");
        effect.setPublished(true); effect.setUpdatedAt(Instant.now()); state.setUpdatedAt(Instant.now());
        stores.state().save(state);
      }
      return null;
    });
  }

  public void recordEffectResult(String key, String effectId, boolean succeeded, Exception error) throws Exception {
    if (succeeded && error != null) throw new IllegalArgumentException("successful result cannot include error");
    transaction.run(stores -> {
      SagaState state = state(stores, key);
      SagaState.EffectState effect = effect(state, effectId);
      if (!SagaState.EFFECT_SUCCEEDED.equals(effect.getStatus()) && !SagaState.EFFECT_FAILED.equals(effect.getStatus())) {
        String failure = succeeded ? "" : (error == null ? "effect failed" : error.getMessage());
        effect.setStatus(succeeded ? SagaState.EFFECT_SUCCEEDED : SagaState.EFFECT_FAILED);
        effect.setLastError(failure); effect.setUpdatedAt(Instant.now());
        record(stores, state, emptyEvent(), succeeded ? COMMAND_SUCCEEDED : COMMAND_FAILED,
            effect.getStepId(), effect.getAttempts(), command(state, effect), failure);
        state.setUpdatedAt(Instant.now()); stores.state().save(state);
      }
      return null;
    });
  }

  public SagaState startCompensation(String key, String stepId, Exception error) throws Exception {
    return transaction.run(stores -> {
      SagaState state = state(stores, key);
      SagaState.CompensationState compensation = state.getCompensation();
      if (compensation == null) compensation = new SagaState.CompensationState(stepId);
      compensation.setStepId(stepId); compensation.setStatus(SagaState.COMPENSATING);
      compensation.setAttempts(compensation.getAttempts() + 1); compensation.setLastError(error == null ? "" : error.getMessage());
      compensation.setUpdatedAt(Instant.now()); state.setCompensation(compensation); state.setStatus(SagaState.COMPENSATING); state.setOutcome("");
      record(stores, state, emptyEvent(), COMPENSATION_STARTED, stepId, compensation.getAttempts(), null, compensation.getLastError());
      stores.state().save(state); return state;
    });
  }

  public SagaState startNewRun(String key) throws Exception {
    return transaction.run(stores -> {
      SagaState state = state(stores, key);
      if (!SagaState.COMPLETED.equals(state.getStatus()) && !SagaState.FAILED.equals(state.getStatus())) {
        throw new IllegalStateException("cannot start a new run while Saga is not terminal");
      }
      state.setRunId(UUID.randomUUID().toString()); state.setNextSequence(0); state.setStatus(SagaState.RUNNING);
      state.setOutcome(""); state.setStepId(""); state.setRetryCount(0); state.setLastError("");
      state.setEffects(new java.util.LinkedHashMap<>()); state.setCompensation(null); state.setUpdatedAt(Instant.now());
      record(stores, state, emptyEvent(), RUN_STARTED, "", null, null, ""); stores.state().save(state); return state;
    });
  }

  public void completeCompensation(String key) throws Exception {
    transaction.run(stores -> {
      SagaState state = state(stores, key); SagaState.CompensationState compensation = compensation(state);
      compensation.setStatus(SagaState.COMPLETED); compensation.setLastError(""); compensation.setUpdatedAt(Instant.now());
      state.setStatus(SagaState.COMPLETED); state.setOutcome(OUTCOME_COMPENSATED); state.setUpdatedAt(Instant.now());
      record(stores, state, emptyEvent(), COMPENSATION_COMPLETED, compensation.getStepId(), compensation.getAttempts(), null, "");
      record(stores, state, emptyEvent(), RUN_COMPENSATED, "", null, null, "");
      stores.state().save(state); return null;
    });
  }

  public void failCompensation(String key, Exception error) throws Exception {
    transaction.run(stores -> {
      SagaState state = state(stores, key); SagaState.CompensationState compensation = compensation(state);
      compensation.setStatus(SagaState.FAILED); compensation.setLastError(error.getMessage()); compensation.setUpdatedAt(Instant.now());
      state.setStatus(SagaState.FAILED); state.setOutcome(OUTCOME_FAILED); state.setUpdatedAt(Instant.now());
      record(stores, state, emptyEvent(), COMPENSATION_FAILED, compensation.getStepId(), compensation.getAttempts(), null, error.getMessage());
      record(stores, state, emptyEvent(), RUN_FAILED, "", null, null, error.getMessage()); stores.state().save(state); return null;
    });
  }

  private void recordRolledBackFailure(String key, SagaEventEnvelope event, Exception cause) {
    try {
      transaction.run(stores -> {
        SagaState state = stores.state().loadForUpdate(definition.sagaType(), key);
        if (state != null) {
          state.setRetryCount(state.getRetryCount() + 1); state.setLastError(cause.getMessage()); state.setUpdatedAt(Instant.now());
          record(stores, state, event, STEP_FAILED, state.getStepId(), state.getRetryCount(), null, cause.getMessage());
          stores.state().save(state);
        }
        stores.inbox().fail(definition.sagaType(), event.eventId(), cause); return null;
      });
    } catch (Exception ignored) { /* preserve original handler failure */ }
  }

  private void record(Stores stores, SagaState state, SagaEventEnvelope source, String type, String step, Integer attempt, SagaCommand command, String error) throws Exception {
    Instant now = Instant.now();
    SagaHistoryEvent event = SagaHistoryEvent.builder().environmentId(options.environmentId()).serviceName(options.serviceName())
        .sagaType(state.getSagaType()).sagaId(state.getSagaId()).runId(state.getRunId()).sequence(state.nextSequence())
        .eventType(type).occurredAt(now).recordedAt(now).stepId(step).attempt(attempt)
        .commandId(command == null ? "" : command.getCommandId()).effectId(command == null ? "" : command.getEffectId())
        .sourceEventId(empty(source.eventId())).correlationId(empty(source.correlationId())).causationId(empty(source.causationId()))
        .sourceTopic(empty(source.sourceTopic())).sourcePartition(source.sourcePartition()).sourceOffset(source.sourceOffset())
        .aggregateType(empty(source.aggregateType())).aggregateId(empty(source.aggregateId())).aggregateVersion(source.aggregateVersion())
        .payload(command == null ? "" : command.getPayload()).error(error).build();
    stores.history().append(event);
  }
  private SagaState state(Stores stores, String key) throws Exception { SagaState state = stores.state().loadForUpdate(definition.sagaType(), key); if (state == null) throw new IllegalArgumentException("Saga state not found"); return state; }
  private static SagaState.EffectState effect(SagaState state, String id) { SagaState.EffectState effect = state.getEffects().get(id); if (effect == null) throw new IllegalArgumentException("effect not found: " + id); return effect; }
  private static SagaState.CompensationState compensation(SagaState state) { if (state.getCompensation() == null) throw new IllegalArgumentException("compensation is not active"); return state.getCompensation(); }
  private static SagaCommand command(SagaState state, SagaState.EffectState effect) { return new SagaCommand(effect.getStepId(), "").effectId(effect.getEffectId()).commandId(effect.getCommandId()).sagaType(state.getSagaType()).sagaId(state.getSagaId()).correlationId(state.getCorrelationId()); }
  private static SagaEventEnvelope emptyEvent() { return new SagaEventEnvelope("", "", "", ""); }
  private static String associationKey(SagaEventEnvelope event) { String key = empty(event.associationKey()); if (!key.isEmpty()) return key; key = empty(event.correlationId()); if (!key.isEmpty()) return key; key = empty(event.aggregateId()); if (key.isEmpty()) throw new IllegalArgumentException("association key is required"); return key; }
  private static String empty(String value) { return value == null ? "" : value; }
  private static boolean blank(String value) { return value == null || value.isBlank(); }
}
