/*
 * Copyright 2021 the original author or authors
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.data.couchbase.core.convert.CouchbaseConverter;
import org.springframework.data.couchbase.core.convert.join.N1qlJoinResolver;
import org.springframework.data.couchbase.core.convert.translation.TranslationService;
import org.springframework.data.couchbase.core.mapping.CouchbaseDocument;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentEntity;
import org.springframework.data.couchbase.core.mapping.CouchbasePersistentProperty;
import org.springframework.data.couchbase.core.mapping.event.AfterSaveEvent;
import org.springframework.data.couchbase.core.mapping.event.CouchbaseMappingEvent;
import org.springframework.data.couchbase.repository.support.MappingCouchbaseEntityInformation;
import org.springframework.data.couchbase.repository.support.TransactionResultHolder;
import org.springframework.data.couchbase.transaction.ClientSession;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;

import com.couchbase.client.core.error.CouchbaseException;

public abstract class AbstractTemplateSupport {

	final CouchbaseConverter converter;
	final MappingContext<? extends CouchbasePersistentEntity<?>, CouchbasePersistentProperty> mappingContext;
	final TranslationService translationService;
	ApplicationContext applicationContext;
	static final Logger LOG = LoggerFactory.getLogger(AbstractTemplateSupport.class);

	public AbstractTemplateSupport(CouchbaseConverter converter, TranslationService translationService) {
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.translationService = translationService;
	}

	abstract ReactiveCouchbaseTemplate getReactiveTemplate();

	public <T> T decodeEntityBase(String id, String source, long cas, Class<T> entityClass,
																TransactionResultHolder txResultHolder, ClientSession session) {
		System.err.println("============== Decoding " +id);
		final CouchbaseDocument converted = new CouchbaseDocument(id);
		converted.setId(id);
		CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entityClass);
		if (cas != 0 && persistentEntity.getVersionProperty() != null) {
			converted.put(persistentEntity.getVersionProperty().getName(), cas);
		}

		T readEntity = converter.read(entityClass, (CouchbaseDocument) translationService.decode(source, converted));
		final ConvertingPropertyAccessor<T> accessor = getPropertyAccessor(readEntity);

		if (persistentEntity.getVersionProperty() != null) {
			accessor.setProperty(persistentEntity.getVersionProperty(), cas);
		}
		if (persistentEntity.transactionResultProperty() != null) {
			accessor.setProperty(persistentEntity.transactionResultProperty(), System.identityHashCode(txResultHolder));
		}
		N1qlJoinResolver.handleProperties(persistentEntity, accessor, getReactiveTemplate(), id);

		if(session != null){
			session.transactionResultHolder(txResultHolder, (T)accessor.getBean());
		}

		return accessor.getBean();
	}

	public <T> T applyResultBase(T entity, CouchbaseDocument converted, Object id, long cas,
			TransactionResultHolder txResultHolder, ClientSession session) {
		ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);

		final CouchbasePersistentEntity<?> persistentEntity = converter.getMappingContext()
				.getRequiredPersistentEntity(entity.getClass());

		final CouchbasePersistentProperty idProperty = persistentEntity.getIdProperty();
		if (idProperty != null) {
			accessor.setProperty(idProperty, id);
		}

		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();
		if (versionProperty != null) {
			accessor.setProperty(versionProperty, cas);
		}

		final CouchbasePersistentProperty transactionResultProperty = persistentEntity.transactionResultProperty();
		if (transactionResultProperty != null) {
			accessor.setProperty(transactionResultProperty, System.identityHashCode(txResultHolder));
		}
		if(session != null){
			session.transactionResultHolder(txResultHolder, (T)accessor.getBean());
		}
		maybeEmitEvent(new AfterSaveEvent(accessor.getBean(), converted));
		return (T) accessor.getBean();

	}

	public Long getCas(final Object entity) {
		final ConvertingPropertyAccessor<Object> accessor = getPropertyAccessor(entity);
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(entity.getClass());
		final CouchbasePersistentProperty versionProperty = persistentEntity.getVersionProperty();

		long cas = 0;
		if (versionProperty != null) {
			Object casObject = accessor.getProperty(versionProperty);
			if (casObject instanceof Number) {
				cas = ((Number) casObject).longValue();
			}
		}
		return cas;
	}

	public String getJavaNameForEntity(final Class<?> clazz) {
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(clazz);
		MappingCouchbaseEntityInformation<?, Object> info = new MappingCouchbaseEntityInformation<>(persistentEntity);
		return info.getJavaType().getName();
	}

	<T> ConvertingPropertyAccessor<T> getPropertyAccessor(final T source) {
		CouchbasePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
		PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
		return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
	}

	public <T> Integer getTxResultKey(T source) {
		final CouchbasePersistentEntity<?> persistentEntity = mappingContext.getRequiredPersistentEntity(source.getClass());
		final CouchbasePersistentProperty transactionResultProperty = persistentEntity.transactionResultProperty();
		if (transactionResultProperty == null) {
			throw new CouchbaseException("the entity class " + source.getClass()
					+ " does not have a property required for transactions:\n\t@TransactionResult TransactionResultHolder txResultHolder");
		}
		return getPropertyAccessor(source).getProperty(transactionResultProperty, Integer.class);
	}

	public void maybeEmitEvent(CouchbaseMappingEvent<?> event) {
		if (canPublishEvent()) {
			try {
				this.applicationContext.publishEvent(event);
			} catch (Exception e) {
				LOG.warn("{} thrown during {}", e, event);
				throw e;
			}
		} else {
			LOG.info("maybeEmitEvent called, but CouchbaseTemplate not initialized with applicationContext");
		}

	}

	private boolean canPublishEvent() {
		return this.applicationContext != null;
	}

	public TranslationService getTranslationService(){
		return translationService;
	}
}
