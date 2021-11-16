/*
 * Copyright 2012-2021 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.couchbase.transactions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.data.couchbase.config.BeanNames;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.couchbase.CouchbaseClientFactory;
import org.springframework.data.couchbase.config.AbstractCouchbaseConfiguration;
import org.springframework.data.couchbase.core.CouchbaseOperations;
import org.springframework.data.couchbase.core.CouchbaseTemplate;
import org.springframework.data.couchbase.core.ReactiveCouchbaseTemplate;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.domain.Person;
import org.springframework.data.couchbase.domain.PersonRepository;
import org.springframework.data.couchbase.domain.ReactivePersonRepository;
import org.springframework.data.couchbase.repository.config.EnableCouchbaseRepositories;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.data.couchbase.transaction.ReactiveCouchbaseTransactionManager;
import org.springframework.data.couchbase.util.Capabilities;
import org.springframework.data.couchbase.util.ClusterType;
import org.springframework.data.couchbase.util.IgnoreWhen;
import org.springframework.data.couchbase.util.JavaIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.transactions.TransactionDurabilityLevel;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;

/**
 * Tests for com.couchbase.transactions without using the spring data transactions framework
 *
 * @author Michael Reiche
 */
@IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.MOCKED)
@SpringJUnitConfig(CouchbaseReactiveTransactionNativeTests.Config.class)
@Transactional(transactionManager = BeanNames.COUCHBASE_TRANSACTION_MANAGER)
public class CouchbasePersonTransactionTests extends JavaIntegrationTests {

	// @Autowired not supported on static fields. These are initialized in beforeAll()
	// Also - @Autowired doesn't work here on couchbaseClientFactory even when it is not static, not sure why - oh, it
	// seems there is not a ReactiveCouchbaseClientFactory bean
	@Autowired CouchbaseClientFactory couchbaseClientFactory;
	@Autowired ReactiveCouchbaseTransactionManager reactiveCouchbaseTransactionManager;
	@Autowired ReactivePersonRepository rxRepo;
	@Autowired PersonRepository repo;
	@Autowired ReactiveCouchbaseTemplate rxCBTmpl;

	static String cName; // short name

	PersonService personService;

	static boolean explode = false;

	static GenericApplicationContext context;
	CouchbaseTemplate operations;

	@BeforeAll
	public static void beforeAll() {
		callSuperBeforeAll(new Object() {});
		// short names
		cName = null;// cName;
		context = new AnnotationConfigApplicationContext(CouchbaseReactiveTransactionNativeTests.Config.class,
				PersonService.class);

	}

	@AfterAll
	public static void afterAll() {
		callSuperAfterAll(new Object() {});
		// context.close();
	}

	@BeforeEach
	public void beforeEachTest() {
		// if(explode) throw new RuntimeException("Explode");
		personService = context.getBean(PersonService.class);
		operations = context.getBean(CouchbaseTemplate.class);
		operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		operations.findByQuery(EventLog.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		operations.removeByQuery(Person.class).all();
		operations.removeByQuery(EventLog.class).all();
		operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();
		operations.findByQuery(EventLog.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).all();

	}

	@Test // DATAMONGO-2265
	public void shouldRollbackAfterException() {

		personService.savePersonErrors(new Person(null, "Walter", "White"));

		operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count();
		// sleepMs(5000);
		explode = true;
		Long count = operations.count(new Query(), Person.class); //
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test // DATAMONGO-2265
	public void shouldRollbackAfterExceptionOfTxAnnotatedMethod() {

		personService.declarativeSavePersonErrors(new Person(null, "Walter", "White"));

		Long count = operations.count(new Query(), Person.class); //
		assertEquals(0, count, "should have done roll back and left 0 entries");
	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntries() {

		personService.savePerson(new Person(null, "Walter", "White"));

		// operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count();
		Long count = operations.count(new Query(), Person.class); //
		assertEquals(1, count, "should have saved and found 1");
	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntriesOfTxAnnotatedMethod() {

		personService.declarativeSavePerson(new Person(null, "Walter", "White"));

		// operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count();

		Long count = operations.count(new Query(), Person.class); //
		assertEquals(1, count, "should have saved and found 1");

	}

	@Test // DATAMONGO-2265
	public void commitShouldPersistTxEntriesAcrossCollections() {

		personService.saveWithLogs(new Person(null, "Walter", "White"));

		// operations.findByQuery(Person.class).withConsistency(QueryScanConsistency.REQUEST_PLUS).count();

		Long count = operations.count(new Query(), Person.class); //
		assertEquals(1, count, "should have saved and found 1");

		Long countEvents = operations.count(new Query(), EventLog.class); //
		assertEquals(4, countEvents, "should have saved and found 4");
	}

	@Test // DATAMONGO-2265
	public void rollbackShouldAbortAcrossCollections() {

		personService.saveWithErrorLogs(new Person(null, "Walter", "White"));

		Long count = operations.count(new Query(), Person.class); //
		assertEquals(0, count, "should have done roll back and left 0 entries");

		Long countEvents = operations.count(new Query(), EventLog.class); //
		assertEquals(0, countEvents, "should have done roll back and left 0 entries");
	}

	@Test // DATAMONGO-2265
	public void countShouldWorkInsideTransaction() {

		personService.countDuringTx(new Person(null, "Walter", "White"));
	}

	@Test // DATAMONGO-2265
	public void emitMultipleElementsDuringTransaction() {
			personService.saveWithLogs(new Person(null, "Walter", "White"));
	}

	@Test // DATAMONGO-2265
	public void errorAfterTxShouldNotAffectPreviousStep() {
		personService.savePerson(new Person(null, "Walter", "White"));
		Long count = operations.count(new Query(), Person.class); //
		assertEquals(1, count, "should have saved and found 1");
	}

	@RequiredArgsConstructor
	static class PersonService {

		final CouchbaseOperations operations;
		final ReactiveCouchbaseTransactionManager manager;

		public Person savePersonErrors(Person person) {
			return operations.insertById(Person.class)
					.one(person);
		}

		public Person savePerson(Person person) {
			return operations.save(person);
		}

		public Long countDuringTx(Person person) {
			operations.save(person);
			return operations.count(new Query(),
					Person.class);
		}

		public void saveWithLogs(Person person) {
			operations.save(new EventLog(new ObjectId().toString(),
					"beforeConvert"));
			operations.save(new EventLog(new ObjectId(),
					"afterConvert"));
			operations.save(new EventLog(new ObjectId(),
					"beforeInsert"));
			operations.save(person);
			operations.save(new EventLog(new ObjectId(),
					"afterInsert"));
			operations.findByQuery(EventLog.class)
					.all();

		}

		public void saveWithErrorLogs(Person person) {
			operations.save(new EventLog(new ObjectId(),
					"beforeConvert")); //
			operations.save(new EventLog(new ObjectId(),
					"afterConvert")); //
			operations.save(new EventLog(new ObjectId(),
					"beforeInsert")); //
			operations.save(person); //
			operations.save(new EventLog(new ObjectId(),
					"afterInsert")); //
			if (1 == 1) throw new RuntimeException("poof!");

		}

		@Transactional
		public Person declarativeSavePerson(Person person) {
			return operations.save(person);
		}

		@Transactional
		public Person declarativeSavePersonBlocking(Person person) {
			return operations.save(person);
		}

		@Transactional
		public void declarativeSavePersonErrors(Person person) {
			operations.save(person); //
			if (1 == 1) new RuntimeException("poof!");
		}
	}
	/*
	  @Test
	  public void deletePersonCBTransactionsRxTmpl() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxCBTmpl.insertById(Person.class).inCollection(cName).one(person).block();
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .then();
	    }));
	    result.block();
	    Person pFound = rxCBTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString()).block();
	    assertNull(pFound, "Should not have found " + pFound);
	  }
	
	  @Test
	  public void deletePersonCBTransactionsRxTmplFail() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .then(rxCBTmpl.removeById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString()))
	          .then();
	    }));
	    assertThrows(TransactionFailed.class, result::block);
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertEquals(pFound, person, "Should have found " + person);
	  }
	
	  //  RxRepo ////////////////////////////////////////////////////////////////////////////////////////////
	
	  @Test
	  public void deletePersonCBTransactionsRxRepo() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxRepo.withCollection(cName).save(person).block();
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString()).then();
	    }));
	    result.block();
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertNull(pFound, "Should not have found " + pFound);
	  }
	
	  @Test
	  public void deletePersonCBTransactionsRxRepoFail() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    rxRepo.withCollection(cName).save(person).block();
	
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString())
	          .then(rxRepo.withCollection(cName).withTransaction(ctx).deleteById(person.getId().toString())).then();
	    }));
	    assertThrows(TransactionFailed.class, result::block);
	    Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	    assertEquals(pFound, person, "Should have found " + person);
	  }
	
	  @Test
	  public void findPersonCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    List<Object> docs = new LinkedList<>();
	    Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
	    Mono<TransactionResult> result = transactions.reactive(((ctx) -> { // get the ctx
	      return rxCBTmpl.findByQuery(Person.class).inCollection(cName).matching(q).transaction(ctx).one().map(doc -> {
	        docs.add(doc);
	        return doc;
	      }).then();
	    }));
	    result.block();
	    assertFalse(docs.isEmpty(), "Should have found " + person);
	    for (Object o : docs) {
	      assertEquals(o, person, "Should have found " + person);
	    }
	  }
	
	  @Test
	  // @Transactional // TODO @Transactional does nothing. Transaction is handled by transactionalOperator
	  // Failed to retrieve PlatformTransactionManager for @Transactional test:
	  public void insertPersonRbCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.insertById(Person.class).inCollection(cName).transaction(ctx).one(person)
	          .<Person> flatMap(it -> Mono.error(new PoofException())).then();
	    });
	
	    try {
	      result.block();
	    } catch (TransactionFailed e) {
	      e.printStackTrace();
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	        assertNull(pFound, "Should not have found " + pFound);
	        return;
	      } else {
	        e.printStackTrace();
	      }
	    }
	    throw new RuntimeException("Should have been a TransactionFailed exception with a cause of PoofException");
	  }
	
	  @Test
	  // @Transactional // TODO @Transactional does nothing. Transaction is handled by transactionalOperator
	  // Failed to retrieve PlatformTransactionManager for @Transactional test:
	  public void replacePersonRbCBTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.findById(Person.class).inCollection(cName).transaction(ctx).one(person.getId().toString())
	          .flatMap(pFound -> rxCBTmpl.replaceById(Person.class).inCollection(cName).transaction(ctx)
	              .one(pFound.withFirstName("Walt")))
	          .<Person> flatMap(it -> Mono.error(new PoofException())).then();
	    });
	
	    try {
	      result.block();
	    } catch (TransactionFailed e) {
	      if (e.getCause() instanceof PoofException) {
	        Person pFound = cbTmpl.findById(Person.class).inCollection(cName).one(person.getId().toString());
	        assertEquals(person, pFound, "Should have found " + person);
	        return;
	      } else {
	        e.printStackTrace();
	      }
	    }
	    throw new RuntimeException("Should have been a TransactionFailed exception with a cause of PoofException");
	  }
	
	  @Test
	  public void findPersonSpringTransactions() {
	    Person person = new Person(1, "Walter", "White");
	    remove(cbTmpl, cName, person.getId().toString());
	    cbTmpl.insertById(Person.class).inCollection(cName).one(person);
	    List<Object> docs = new LinkedList<>();
	    Query q = Query.query(QueryCriteria.where("meta().id").eq(person.getId()));
	    Mono<TransactionResult> result = transactions.reactive((ctx) -> { // get the ctx
	      return rxCBTmpl.findByQuery(Person.class).inCollection(cName).matching(q).transaction(ctx).one().map(doc -> {
	        docs.add(doc);
	        return doc;
	      }).then();
	    });
	    result.block();
	    assertFalse(docs.isEmpty(), "Should have found " + person);
	    for (Object o : docs) {
	      assertEquals(o, person, "Should have found " + person);
	    }
	  }
	*/
	void remove(Collection col, String id) {
		remove(col.reactive(), id);
	}

	void remove(ReactiveCollection col, String id) {
		try {
			col.remove(id, RemoveOptions.removeOptions().timeout(Duration.ofSeconds(10))).block();
		} catch (DocumentNotFoundException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	void remove(CouchbaseTemplate template, String collection, String id) {
		remove(template.reactive(), collection, id);
	}

	void remove(ReactiveCouchbaseTemplate template, String collection, String id) {
		try {
			template.removeById(Person.class).inCollection(collection).one(id).block();
			System.out.println("removed " + id);
		} catch (DocumentNotFoundException | DataRetrievalFailureException nfe) {
			System.out.println(id + " : " + "DocumentNotFound when deleting");
		}
	}

	static class PoofException extends Exception {};

	@Configuration
	@EnableCouchbaseRepositories("org.springframework.data.couchbase")
	@EnableReactiveCouchbaseRepositories("org.springframework.data.couchbase")
	static class Config extends AbstractCouchbaseConfiguration {

		@Override
		public String getConnectionString() {
			return connectionString();
		}

		@Override
		public String getUserName() {
			return config().adminUsername();
		}

		@Override
		public String getPassword() {
			return config().adminPassword();
		}

		@Override
		public String getBucketName() {
			return bucketName();
		}

		@Override
		public TransactionConfig transactionConfig() {
			return TransactionConfigBuilder.create().logDirectly(Event.Severity.INFO).logOnFailure(true, Event.Severity.ERROR)
					.expirationTime(Duration.ofMinutes(10)).durabilityLevel(TransactionDurabilityLevel.MAJORITY).build();
		}

	}

	@Data
	// @AllArgsConstructor
	static class EventLog {
		public EventLog() {};

		public EventLog(ObjectId oid, String action) {
			this.id = oid.toString();
			this.action = action;
		}

		public EventLog(String id, String action) {
			this.id = id;
			this.action = action;
		}

		String id;
		String action;
	}
}
