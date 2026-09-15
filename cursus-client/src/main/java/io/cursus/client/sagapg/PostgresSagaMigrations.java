package io.cursus.client.sagapg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/** Applies the idempotent, service-owned PostgreSQL Saga schema. */
public final class PostgresSagaMigrations {
  private PostgresSagaMigrations() {}

  public static void migrate(DataSource dataSource) throws SQLException {
    execute(dataSource, "/io/cursus/client/sagapg/001_saga_history_v1.sql");
  }

  static void execute(DataSource dataSource, String resource) throws SQLException {
    String schema;
    try (var input = PostgresSagaMigrations.class.getResourceAsStream(resource)) {
      if (input == null) throw new IllegalStateException("missing migration " + resource);
      schema = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new IllegalStateException("read migration", exception);
    }
    try (Connection connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute(schema);
    }
  }
}
