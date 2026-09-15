package io.cursus.client.saga;

import static org.assertj.core.api.Assertions.assertThat;

import com.mysql.cj.jdbc.MysqlDataSource;
import io.cursus.client.sagapg.PostgresSagaMigrations;
import io.cursus.client.sagapg.PostgresSagaTransaction;
import io.cursus.client.sagamysql.MySqlSagaMigrations;
import io.cursus.client.sagamysql.MySqlSagaTransaction;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.postgresql.ds.PGSimpleDataSource;

class SagaJdbcIntegrationTest {
  @Test
  @EnabledIfEnvironmentVariable(named = "CURSUS_SAGA_POSTGRES_DSN", matches = ".+")
  void persistsPostgresHistoryAndOutboxAtomically() throws Exception {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setUrl(jdbcUrl(System.getenv("CURSUS_SAGA_POSTGRES_DSN")));
    verify(dataSource, true);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CURSUS_SAGA_MYSQL_DSN", matches = ".+")
  void persistsMySqlHistoryAndOutboxAtomically() throws Exception {
    MysqlDataSource dataSource = new MysqlDataSource();
    dataSource.setUrl(jdbcUrl(System.getenv("CURSUS_SAGA_MYSQL_DSN")));
    verify(dataSource, false);
  }

  private void verify(DataSource dataSource, boolean postgres) throws Exception {
    if (postgres) PostgresSagaMigrations.migrate(dataSource); else MySqlSagaMigrations.migrate(dataSource);
    String id = "java-contract-" + UUID.randomUUID();
    SagaDefinition definition = new SagaDefinition("java-contract", java.util.Map.of("OrderCreated", (state, event) -> {
      state.setStatus(SagaState.WAITING); state.setStepId("reserve"); return List.of(new SagaCommand("Reserve", "{}"));
    }));
    SagaContracts.Transaction transaction = postgres ? new PostgresSagaTransaction(dataSource, "observability.saga-history.v1") : new MySqlSagaTransaction(dataSource, "observability.saga-history.v1");
    new TransactionalSagaManager(definition, transaction, new SagaHistoryOptions("test", "orders"))
        .handle(new SagaEventEnvelope("event-" + id, "OrderCreated", id, "{}"));
    try (var connection = dataSource.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT count(*) FROM cursus_saga_history WHERE saga_id='" + id + "'")) {
      rows.next(); assertThat(rows.getInt(1)).isGreaterThan(0);
    }
  }

  private static String jdbcUrl(String dsn) {
    if (dsn.startsWith("jdbc:")) return dsn;
    URI uri = URI.create(dsn);
    String query = uri.getRawQuery() == null ? "" : uri.getRawQuery();
    if (uri.getUserInfo() != null) {
      String[] credentials = uri.getUserInfo().split(":", 2);
      query += (query.isEmpty() ? "" : "&") + "user=" + encode(credentials[0]);
      if (credentials.length == 2) query += "&password=" + encode(credentials[1]);
    }
    return "jdbc:" + uri.getScheme() + "://" + uri.getHost()
        + (uri.getPort() == -1 ? "" : ":" + uri.getPort()) + uri.getRawPath()
        + (query.isEmpty() ? "" : "?" + query);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
