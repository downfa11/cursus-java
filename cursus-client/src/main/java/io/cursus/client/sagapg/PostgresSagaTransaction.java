package io.cursus.client.sagapg;
import io.cursus.client.saga.SagaContracts;
import io.cursus.client.saga.jdbc.JdbcSagaTransaction;
import javax.sql.DataSource;
/** PostgreSQL transaction adapter; requires a service-configured history topic. */
public final class PostgresSagaTransaction implements SagaContracts.Transaction {
  private final JdbcSagaTransaction delegate;
  public PostgresSagaTransaction(DataSource dataSource,String historyTopic){delegate=new JdbcSagaTransaction(dataSource,historyTopic,JdbcSagaTransaction.Dialect.POSTGRES);}
  @Override public <T> T run(SagaContracts.Operation<T> operation) throws Exception{return delegate.run(operation);}
}
