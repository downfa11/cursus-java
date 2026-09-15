package io.cursus.client.sagamysql;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/** Applies the idempotent MySQL 8+ Saga schema without imposing a JDBC driver. */
public final class MySqlSagaMigrations {
  private MySqlSagaMigrations() {}
  public static void migrate(DataSource dataSource) throws SQLException {
    String schema;
    try (var input = MySqlSagaMigrations.class.getResourceAsStream("/io/cursus/client/sagamysql/001_saga_history_v1.sql")) {
      if (input == null) throw new IllegalStateException("missing MySQL Saga migration");
      schema = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException exception) { throw new IllegalStateException("read migration", exception); }
    try (Connection connection = dataSource.getConnection(); var statement = connection.createStatement()) {
      for (String sql : schema.split(";")) if (!sql.isBlank()) statement.execute(sql);
    }
  }
}
