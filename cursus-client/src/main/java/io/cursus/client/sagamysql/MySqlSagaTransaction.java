package io.cursus.client.sagamysql;
import io.cursus.client.saga.SagaContracts;
import io.cursus.client.saga.jdbc.JdbcSagaTransaction;
import javax.sql.DataSource;
/** MySQL 8+ transaction adapter; requires a service-configured history topic. */
public final class MySqlSagaTransaction implements SagaContracts.Transaction {
  private final JdbcSagaTransaction delegate;
  public MySqlSagaTransaction(DataSource dataSource,String historyTopic){delegate=new JdbcSagaTransaction(dataSource,historyTopic,JdbcSagaTransaction.Dialect.MYSQL);}
  @Override public <T> T run(SagaContracts.Operation<T> operation) throws Exception{return delegate.run(operation);}
}
