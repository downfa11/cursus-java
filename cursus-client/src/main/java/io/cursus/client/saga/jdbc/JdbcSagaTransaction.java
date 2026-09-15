package io.cursus.client.saga.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cursus.client.saga.SagaCommand;
import io.cursus.client.saga.SagaContracts;
import io.cursus.client.saga.SagaHistoryEvent;
import io.cursus.client.saga.SagaState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

/** JDBC implementation shared by the PostgreSQL and MySQL dialect wrappers. */
public final class JdbcSagaTransaction implements SagaContracts.Transaction {
  public enum Dialect {
    POSTGRES,
    MYSQL
  }

  private static final ObjectMapper JSON = new ObjectMapper();
  private final DataSource dataSource;
  private final String topic;
  private final Dialect dialect;

  public JdbcSagaTransaction(DataSource dataSource, String historyTopic, Dialect dialect) {
    if (dataSource == null || historyTopic == null || historyTopic.isBlank())
      throw new IllegalArgumentException("dataSource and historyTopic are required");
    this.dataSource = dataSource;
    this.topic = historyTopic;
    this.dialect = dialect;
  }

  @Override
  public <T> T run(SagaContracts.Operation<T> operation) throws Exception {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      try {
        T result =
            operation.apply(
                new SagaContracts.Stores(
                    new Store(connection),
                    new Store(connection),
                    new Store(connection),
                    new Store(connection)));
        connection.commit();
        return result;
      } catch (Exception error) {
        connection.rollback();
        throw error;
      } finally {
        if (dialect == Dialect.MYSQL)
          try (var statement = connection.createStatement()) {
            statement.execute("SELECT RELEASE_ALL_LOCKS()");
          } catch (SQLException ignored) {
          }
      }
    }
  }

  private final class Store
      implements SagaContracts.InboxStore,
          SagaContracts.StateStore,
          SagaContracts.CommandOutboxStore,
          SagaContracts.HistoryStore {
    private final Connection c;

    Store(Connection c) {
      this.c = c;
    }

    @Override
    public boolean claim(String name, String id) throws SQLException {
      String sql =
          dialect == Dialect.POSTGRES
              ? "INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,updated_at) VALUES (?,?, 'CLAIMED',NOW()) ON CONFLICT DO NOTHING"
              : "INSERT IGNORE INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) VALUES (?,?,'CLAIMED','',UTC_TIMESTAMP(6))";
      try (var s = c.prepareStatement(sql)) {
        s.setString(1, name);
        s.setString(2, id);
        return s.executeUpdate() == 1;
      }
    }

    @Override
    public void complete(String name, String id) throws SQLException {
      exec(
          "UPDATE cursus_saga_inbox SET status='COMPLETED',updated_at="
              + now()
              + " WHERE consumer_name=? AND event_id=?",
          name,
          id);
    }

    @Override
    public void fail(String name, String id, Exception error) throws SQLException {
      String sql =
          dialect == Dialect.POSTGRES
              ? "INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) VALUES (?,?,'FAILED',?,NOW()) ON CONFLICT (consumer_name,event_id) DO UPDATE SET status='FAILED',last_error=EXCLUDED.last_error,updated_at=EXCLUDED.updated_at"
              : "INSERT INTO cursus_saga_inbox (consumer_name,event_id,status,last_error,updated_at) VALUES (?,?,'FAILED',?,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE status='FAILED',last_error=VALUES(last_error),updated_at=VALUES(updated_at)";
      exec(sql, name, id, error.getMessage());
    }

    @Override
    public SagaState loadForUpdate(String type, String id) throws Exception {
      if (dialect == Dialect.POSTGRES) {
        try (var statement =
            c.prepareStatement("SELECT pg_advisory_xact_lock(hashtext(?),hashtext(?))")) {
          statement.setString(1, type);
          statement.setString(2, id);
          try (ResultSet ignored = statement.executeQuery()) {}
        }
      } else {
        try (var statement = c.prepareStatement("SELECT GET_LOCK(SHA2(CONCAT(?,':',?),256),10)")) {
          statement.setString(1, type);
          statement.setString(2, id);
          try (ResultSet rows = statement.executeQuery()) {
            if (!rows.next() || rows.getInt(1) != 1) throw new SQLException("Saga lock timeout");
          }
        }
      }
      try (var s =
          c.prepareStatement(
              "SELECT association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at FROM cursus_saga_state WHERE saga_type=? AND saga_id=? FOR UPDATE")) {
        s.setString(1, type);
        s.setString(2, id);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) return null;
          SagaState v = new SagaState(id, type, r.getString(1));
          v.setCorrelationId(r.getString(2));
          v.setRunId(r.getString(3));
          v.setNextSequence(r.getLong(4));
          v.setStatus(r.getString(5));
          v.setOutcome(r.getString(6));
          v.setStepId(r.getString(7));
          v.setData(JSON.readValue(r.getString(8), new TypeReference<>() {}));
          v.setRetryCount(r.getInt(9));
          v.setLastError(r.getString(10));
          Map<String, Map<String, Object>> raw =
              JSON.readValue(r.getString(11), new TypeReference<>() {});
          for (var entry : raw.entrySet()) {
            Map<String, Object> x = entry.getValue();
            SagaState.EffectState effect =
                new SagaState.EffectState(entry.getKey(), (String) x.get("step_id"));
            effect.setStatus((String) x.get("status"));
            effect.setCommandId((String) x.get("command_id"));
            effect.setPublished(Boolean.TRUE.equals(x.get("published")));
            effect.setAttempts(((Number) x.get("attempts")).intValue());
            effect.setLastError((String) x.get("last_error"));
            v.getEffects().put(entry.getKey(), effect);
          }
          if (r.getString(12) != null) {
            Map<String, Object> x = JSON.readValue(r.getString(12), new TypeReference<>() {});
            SagaState.CompensationState q =
                new SagaState.CompensationState((String) x.get("step_id"));
            q.setStatus((String) x.get("status"));
            q.setAttempts(((Number) x.get("attempts")).intValue());
            q.setLastError((String) x.get("last_error"));
            v.setCompensation(q);
          }
          v.setUpdatedAt(r.getTimestamp(13).toInstant());
          return v;
        }
      }
    }

    @Override
    public void save(SagaState v) throws Exception {
      String sql =
          dialect == Dialect.POSTGRES
              ? "INSERT INTO cursus_saga_state (saga_type,saga_id,association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at) VALUES (?,?,?,?,?::uuid,?,?,?,?,?::jsonb,?,?,?::jsonb,?::jsonb,?) ON CONFLICT (saga_type,saga_id) DO UPDATE SET correlation_id=EXCLUDED.correlation_id,run_id=EXCLUDED.run_id,next_sequence=EXCLUDED.next_sequence,status=EXCLUDED.status,outcome=EXCLUDED.outcome,step_id=EXCLUDED.step_id,data=EXCLUDED.data,retry_count=EXCLUDED.retry_count,last_error=EXCLUDED.last_error,effects=EXCLUDED.effects,compensation=EXCLUDED.compensation,updated_at=EXCLUDED.updated_at"
              : "INSERT INTO cursus_saga_state (saga_type,saga_id,association_key,correlation_id,run_id,next_sequence,status,outcome,step_id,data,retry_count,last_error,effects,compensation,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE correlation_id=VALUES(correlation_id),run_id=VALUES(run_id),next_sequence=VALUES(next_sequence),status=VALUES(status),outcome=VALUES(outcome),step_id=VALUES(step_id),data=VALUES(data),retry_count=VALUES(retry_count),last_error=VALUES(last_error),effects=VALUES(effects),compensation=VALUES(compensation),updated_at=VALUES(updated_at)";
      Map<String, Object> effects = new LinkedHashMap<>();
      for (var e : v.getEffects().entrySet()) {
        SagaState.EffectState x = e.getValue();
        effects.put(
            e.getKey(),
            Map.of(
                "step_id",
                x.getStepId(),
                "status",
                x.getStatus(),
                "command_id",
                x.getCommandId(),
                "published",
                x.isPublished(),
                "attempts",
                x.getAttempts(),
                "last_error",
                x.getLastError()));
      }
      Map<String, Object> comp = null;
      if (v.getCompensation() != null) {
        SagaState.CompensationState x = v.getCompensation();
        comp =
            Map.of(
                "step_id",
                x.getStepId(),
                "status",
                x.getStatus(),
                "attempts",
                x.getAttempts(),
                "last_error",
                x.getLastError());
      }
      exec(
          sql,
          v.getSagaType(),
          v.getSagaId(),
          v.getAssociationKey(),
          v.getCorrelationId(),
          v.getRunId(),
          v.getNextSequence(),
          v.getStatus(),
          v.getOutcome(),
          v.getStepId(),
          JSON.writeValueAsString(v.getData()),
          v.getRetryCount(),
          v.getLastError(),
          JSON.writeValueAsString(effects),
          comp == null ? null : JSON.writeValueAsString(comp),
          Timestamp.from(v.getUpdatedAt()));
    }

    @Override
    public void enqueue(SagaCommand x) throws SQLException {
      String sql =
          dialect == Dialect.POSTGRES
              ? "INSERT INTO cursus_saga_outbox (command_id,saga_type,saga_id,effect_id,command_type,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?::jsonb,NOW()) ON CONFLICT (command_id) DO NOTHING"
              : "INSERT IGNORE INTO cursus_saga_outbox (command_id,saga_type,saga_id,effect_id,command_type,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?, ?,UTC_TIMESTAMP(6))";
      exec(
          sql,
          x.getCommandId(),
          x.getSagaType(),
          x.getSagaId(),
          x.getEffectId(),
          x.getType(),
          x.getCorrelationId(),
          x.getCausationId(),
          x.getPayload().isEmpty() ? "{}" : x.getPayload());
    }

    @Override
    public void append(SagaHistoryEvent e) throws Exception {
      String sql =
          dialect == Dialect.POSTGRES
              ? "INSERT INTO cursus_saga_history (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,run_id,sequence,event_type,occurred_at,recorded_at,step_id,attempt,command_id,effect_id,source_event_id,correlation_id,causation_id,source_topic,source_partition,source_offset,aggregate_type,aggregate_id,aggregate_version,payload,error) VALUES (?::uuid,?,?,?,?,?,?::uuid,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
              : "INSERT INTO cursus_saga_history (history_event_id,history_schema_version,environment_id,service_name,saga_type,saga_id,run_id,sequence,event_type,occurred_at,recorded_at,step_id,attempt,command_id,effect_id,source_event_id,correlation_id,causation_id,source_topic,source_partition,source_offset,aggregate_type,aggregate_id,aggregate_version,payload,error) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
      exec(
          sql,
          e.getHistoryEventId(),
          e.getHistorySchemaVersion(),
          e.getEnvironmentId(),
          e.getServiceName(),
          e.getSagaType(),
          e.getSagaId(),
          e.getRunId(),
          e.getSequence(),
          e.getEventType(),
          Timestamp.from(e.getOccurredAt()),
          Timestamp.from(e.getRecordedAt()),
          e.getStepId(),
          e.getAttempt(),
          e.getCommandId(),
          e.getEffectId(),
          e.getSourceEventId(),
          e.getCorrelationId(),
          e.getCausationId(),
          e.getSourceTopic(),
          e.getSourcePartition(),
          e.getSourceOffset(),
          e.getAggregateType(),
          e.getAggregateId(),
          e.getAggregateVersion(),
          e.getPayload(),
          e.getError());
      String out =
          dialect == Dialect.POSTGRES
              ? "INSERT INTO cursus_saga_history_outbox (history_event_id,topic_name,payload) VALUES (?::uuid,? ,?::jsonb)"
              : "INSERT INTO cursus_saga_history_outbox (history_event_id,topic_name,payload,status,attempts,last_error,created_at) VALUES (?,?,?,'PENDING',0,'',UTC_TIMESTAMP(6))";
      exec(out, e.getHistoryEventId(), topic, e.toJson());
    }

    private void exec(String sql, Object... values) throws SQLException {
      try (PreparedStatement s = c.prepareStatement(sql)) {
        for (int i = 0; i < values.length; i++) s.setObject(i + 1, values[i]);
        s.executeUpdate();
      }
    }

    private String now() {
      return dialect == Dialect.POSTGRES ? "NOW()" : "UTC_TIMESTAMP(6)";
    }
  }
}
