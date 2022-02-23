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
package org.springframework.data.couchbase.core;

import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.query.OptionsBuilder;
import org.springframework.data.couchbase.core.support.PseudoArgs;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.couchbase.transaction.CouchbaseStuffHandle;
import org.springframework.util.Assert;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.transactions.AttemptContextReactive;
import com.couchbase.transactions.TransactionGetResult;
import com.couchbase.transactions.components.TransactionLinks;
import com.example.demo.CouchbaseTransactionalTemplate;

public class ReactiveReplaceByIdOperationSupport implements ReactiveReplaceByIdOperation {

	private final ReactiveCouchbaseTemplate template;
	private static final Logger LOG = LoggerFactory.getLogger(ReactiveReplaceByIdOperationSupport.class);

	public ReactiveReplaceByIdOperationSupport(final ReactiveCouchbaseTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveReplaceById<T> replaceById(final Class<T> domainType) {
		Assert.notNull(domainType, "DomainType must not be null!");
		return new ReactiveReplaceByIdSupport<>(template, domainType, null, null, null, PersistTo.NONE, ReplicateTo.NONE,
				DurabilityLevel.NONE, null, null, template.support());
	}

	static class ReactiveReplaceByIdSupport<T> implements ReactiveReplaceById<T> {

		private final ReactiveCouchbaseTemplate template;
		private final Class<T> domainType;
		private final String scope;
		private final String collection;
		private final ReplaceOptions options;
		private final PersistTo persistTo;
		private final ReplicateTo replicateTo;
		private final DurabilityLevel durabilityLevel;
		private final Duration expiry;
		private final CouchbaseStuffHandle txCtx;
		private final ReactiveTemplateSupport support;

		private final TransactionLinks tl = new TransactionLinks(Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
				Optional.empty(), Optional.empty(), false, Optional.empty(), Optional.empty());

		ReactiveReplaceByIdSupport(final ReactiveCouchbaseTemplate template, final Class<T> domainType, final String scope,
				final String collection, final ReplaceOptions options, final PersistTo persistTo, final ReplicateTo replicateTo,
				final DurabilityLevel durabilityLevel, final Duration expiry, final CouchbaseStuffHandle txCtx,
				ReactiveTemplateSupport support) {
			this.template = template;
			this.domainType = domainType;
			this.scope = scope;
			this.collection = collection;
			this.options = options;
			this.persistTo = persistTo;
			this.replicateTo = replicateTo;
			this.durabilityLevel = durabilityLevel;
			this.expiry = expiry;
			this.txCtx = txCtx;
			this.support = support;
		}

		@Override
		public Mono<T> one(T object) {
			PseudoArgs<ReplaceOptions> pArgs = new PseudoArgs<>(template, scope, collection, options, txCtx, domainType);
			LOG.trace("replaceById {}", pArgs);
			Mono<ReactiveCouchbaseTemplate> tmpl = template.doGetTemplate();
			AttemptContextReactive ctx = CouchbaseTransactionalTemplate.getContextReactive(template);
			ClientSession session = CouchbaseTransactionalTemplate.getSession(template);
			Mono<T> reactiveEntity;
			if (ctx != null) {
				reactiveEntity = support.encodeEntity(object).flatMap(converted -> template.getCouchbaseClientFactory()
						.withScope(pArgs.getScope()).getCollection(pArgs.getCollection()).flatMap(
								col -> ctx.replace(session.transactionResultHolder(getTransactionHolder(object)).transactionGetResult(),
										converted.getContent()
								/*(GetOptions) pArgs.getOptions() */).flatMap(result -> {
									Mono<T> t =support.applyResult(object, converted, converted.getId(), result.cas(), new TransactionResultHolder(result), session);

									return t;
								})));
			} else {
				reactiveEntity = support.encodeEntity(object)
						.flatMap(converted -> tmpl.flatMap(tp -> tp.getCouchbaseClientFactory().getSession(null).flatMap(s -> {
							if (s == null || s.getAttemptContextReactive() == null) {
								return template.getCouchbaseClientFactory().withScope(pArgs.getScope())
										.getCollection(pArgs.getCollection())
										.flatMap(collection -> collection.reactive()
												.replace(converted.getId(), converted.export(),
														buildReplaceOptions(pArgs.getOptions(), object, converted))
												.flatMap(
														result -> support.applyResult(object, converted, converted.getId(), result.cas(), null)));
							} else {
								TransactionGetResult getResult = s.transactionResultHolder(getTransactionHolder(object))
										.transactionGetResult();
								return s.getAttemptContextReactive().replace(getResult, converted.getContent())
										.flatMap(result -> {
											Mono<T> t =support.applyResult(object, converted, converted.getId(), result.cas(), null);
											//session.transactionResultHolder(result, t);
											return t;
										});
							}
						})));
			}
			/*
			ReactiveCollection rc = clientFactory.withScope(pArgs.getScope()).getCollection(pArgs.getCollection()).block().reactive();
			Mono<T> reactiveEntity;
			if (pArgs.getTxOp() == null) {
				reactiveEntity = support.encodeEntity(object)
						.flatMap(converted -> rc
								.replace(converted.getId(), converted.export(),
										buildReplaceOptions(pArgs.getOptions(), object, converted))
								.flatMap(result -> support.applyResult(object, converted, converted.getId(), result.cas(), null)));
			
			} else {
				TransactionGetResult getResult = pArgs.getTxOp().transactionResultHolder(getTransactionHolder(object)).transactionGetResult();
				reactiveEntity = support.encodeEntity(object)
						.flatMap(converted -> pArgs.getTxOp().getAttemptContextReactive()
								.replace( getResult,
										converted.getContent())
								.flatMap(result -> support.applyResult(object, converted, converted.getId(), result.cas(),
										pArgs.getTxOp().transactionResultHolder(result))));
			
			}
			*/

			return reactiveEntity.onErrorMap(throwable -> {
				if (throwable instanceof RuntimeException) {
					return template.potentiallyConvertRuntimeException((RuntimeException) throwable);
				} else {
					return throwable;
				}
			});
		}

		private <T> Integer getTransactionHolder(T object) {
			Integer transactionResultHolder;
			System.err.println("GET: "+System.identityHashCode(object)+" "+object);
      if(1==1){
      	return System.identityHashCode(object);
			}
			transactionResultHolder = template.support().getTxResultHolder(object);
			if (transactionResultHolder == null) {
				throw new CouchbaseException(
						"TransactionResult from entity is null - was the entity obtained in a transaction?");
			}
			return transactionResultHolder;
		}

		private Object buildTransactionOptions(ReplaceOptions options, T object, CouchbaseDocument doc) {
			return OptionsBuilder.buildTransactionReplaceOptions(buildReplaceOptions(options, object, doc));
		}

		@Override
		public Flux<? extends T> all(Collection<? extends T> objects) {
			return Flux.fromIterable(objects).flatMap(this::one);
		}

		private ReplaceOptions buildReplaceOptions(ReplaceOptions options, T object, CouchbaseDocument doc) {
			return OptionsBuilder.buildReplaceOptions(options, persistTo, replicateTo, durabilityLevel, expiry,
					support.getCas(object), doc);
		}

		@Override
		public TerminatingReplaceById<T> withOptions(final ReplaceOptions options) {
			Assert.notNull(options, "Options must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdTxOrNot<T> inCollection(final String collection) {
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdInCollection<T> inScope(final String scope) {
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdInCollection<T> withDurability(final DurabilityLevel durabilityLevel) {
			Assert.notNull(durabilityLevel, "Durability Level must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdInCollection<T> withDurability(final PersistTo persistTo, final ReplicateTo replicateTo) {
			Assert.notNull(persistTo, "PersistTo must not be null.");
			Assert.notNull(replicateTo, "ReplicateTo must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdWithDurability<T> withExpiry(final Duration expiry) {
			Assert.notNull(expiry, "expiry must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

		@Override
		public ReplaceByIdWithExpiry<T> transaction(final CouchbaseStuffHandle txCtx) {
			Assert.notNull(txCtx, "txCtx must not be null.");
			return new ReactiveReplaceByIdSupport<>(template, domainType, scope, collection, options, persistTo, replicateTo,
					durabilityLevel, expiry, txCtx, support);
		}

	}

}
