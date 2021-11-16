package org.springframework.data.couchbase.transaction;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.transactions.AttemptContext;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.TransactionInsertOptions;
import com.couchbase.transactions.TransactionQueryOptions;
import com.couchbase.transactions.TransactionResult;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

//@Aspect
public class CbTransactionManager extends AbstractPlatformTransactionManager implements DisposableBean, ResourceTransactionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTransactionManager.class);

  private final CouchbaseTemplate template;
  private final Transactions transactions;

  public CbTransactionManager(CouchbaseTemplate template, TransactionConfig transactionConfig) {
    this.template = template;
    this.transactions = Transactions.create(
        template.getCouchbaseClientFactory().getCluster(),
        transactionConfig
    );
  }

 // @Around("@annotation(org.springframework.transaction.annotation.Transactional)")
  public Object runInsideTransactionsClosure(Object joinPoint) {
    final AtomicReference<Object> result = new AtomicReference<>();
    TransactionResult txnResult = transactions.run(attemptContext -> {
      if (TransactionSynchronizationManager.hasResource(template.getCouchbaseClientFactory())) {
        ((CouchbaseResourceHolder) TransactionSynchronizationManager
            .getResource(template.getCouchbaseClientFactory()))
            .setAttemptContext(attemptContext);
      } else {
        TransactionSynchronizationManager.bindResource(
            template.getCouchbaseClientFactory(),
            new CouchbaseResourceHolder(attemptContext)
        );
      }

      try {
        // Since we are on a different thread now transparently, at least make sure
        // that the original method invocation is synchronized.
        synchronized (this) {
          result.set(joinPoint /*joinPoint.proceed()*/);
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });

    LOGGER.debug("Completed Couchbase Transaction with Result: " + txnResult);
    return result.get();
  }

  @Override
  protected CouchbaseTransactionObject doGetTransaction() throws TransactionException {
    CouchbaseResourceHolder resourceHolder = (CouchbaseResourceHolder) TransactionSynchronizationManager
        .getResource(template.getCouchbaseClientFactory());
    return new CouchbaseTransactionObject(resourceHolder);
  }

  @Override
  protected boolean isExistingTransaction(Object transaction) throws TransactionException {
    return extractTransaction(transaction).hasResourceHolder();
  }

  @Override
  protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
    LOGGER.debug("Beginning Couchbase Transaction with Definition {}", definition);
  }

  @Override
  protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
    LOGGER.debug("Committing Couchbase Transaction with status {}", status);
  }

  @Override
  protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
    LOGGER.warn("Rolling back Couchbase Transaction with status {}", status);
  }

  @Override
  protected void doCleanupAfterCompletion(Object transaction) {
    LOGGER.trace("Performing cleanup of Couchbase Transaction {}", transaction);
  }

  public TransactionGetResult get(String id) {
    Collection collection = template.getCouchbaseClientFactory().getDefaultCollection();
    return run(ctx -> ctx.get(collection, id));
  }

  public TransactionGetResult insert(String id, Object content) {
    return insert(id, content, TransactionInsertOptions.DEFAULT);
  }

  public TransactionGetResult insert(String id, Object content, TransactionInsertOptions options) {
    Collection collection = template.getCouchbaseClientFactory().getDefaultCollection();
    return run(ctx -> ctx.insert(collection, id, content, options));
  }

  public QueryResult query(String statement) {
    return query(statement, TransactionQueryOptions.queryOptions());
  }

  public QueryResult query(String statement, TransactionQueryOptions options) {
    return run(ctx -> ctx.query(statement, options));
  }

  private <T> T run(Function<AttemptContext, T> func) {
    CouchbaseResourceHolder resource = (CouchbaseResourceHolder) TransactionSynchronizationManager
        .getResource(template.getCouchbaseClientFactory());

    if (resource == null) {
      throw new NoTransactionException("No active transaction found. Maybe @Transactional is not present?");
    }

    return func.apply(resource.getAttemptContext());
  }

  @Override
  public void destroy() {
    transactions.close();
  }

  @Override
  public Object getResourceFactory() {
    return template.getCouchbaseClientFactory();
  }

  private static CouchbaseTransactionObject extractTransaction(Object transaction) {
    Assert.isInstanceOf(CouchbaseTransactionObject.class, transaction,
        () -> String.format("Expected to find a %s but it turned out to be %s.", CouchbaseTransactionObject.class,
            transaction.getClass()));

    return (CouchbaseTransactionObject) transaction;
  }

  public static class CouchbaseResourceHolder extends ResourceHolderSupport {

    private volatile AttemptContext attemptContext;

    public CouchbaseResourceHolder(AttemptContext attemptContext) {
      this.attemptContext = attemptContext;
    }

    public AttemptContext getAttemptContext() {
      return attemptContext;
    }

    public void setAttemptContext(AttemptContext attemptContext) {
      this.attemptContext = attemptContext;
    }

    @Override
    public String toString() {
      return "CouchbaseResourceHolder{" +
          "attemptContext=" + attemptContext +
          '}';
    }
  }

  protected static class CouchbaseTransactionObject implements SmartTransactionObject {

    private final CouchbaseResourceHolder resourceHolder;

    CouchbaseTransactionObject(CouchbaseResourceHolder resourceHolder) {
      this.resourceHolder = resourceHolder;
    }

    @Override
    public boolean isRollbackOnly() {
      return this.resourceHolder != null && this.resourceHolder.isRollbackOnly();
    }

    @Override
    public void flush() {
      TransactionSynchronizationUtils.triggerFlush();
    }

    public boolean hasResourceHolder() {
      return resourceHolder != null;
    }

    @Override
    public String toString() {
      return "CouchbaseTransactionObject{" +
          "resourceHolder=" + resourceHolder +
          '}';
    }
  }

}
