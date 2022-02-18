/*
 * Copyright 2012-2022 the original author or authors
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
package com.querydsl.couchbase.document;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.springframework.data.couchbase.core.query.QueryCriteria;
import org.springframework.data.couchbase.core.query.QueryCriteriaDefinition;
import org.springframework.data.couchbase.repository.support.DBRef;
import org.springframework.data.domain.Sort;

import com.querydsl.core.types.Constant;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.FactoryExpression;
import com.querydsl.core.types.Operation;
import com.querydsl.core.types.Operator;
import com.querydsl.core.types.Ops;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.ParamExpression;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.PathMetadata;
import com.querydsl.core.types.PathType;
import com.querydsl.core.types.SubQueryExpression;
import com.querydsl.core.types.TemplateExpression;
import com.querydsl.core.types.Visitor;

/**
 * Serializes the given Querydsl query to a Document query for Couchbase.
 *
 * @author Michael Reiche
 */
public abstract class CouchbaseDocumentSerializer implements Visitor<Object, Void> {

	boolean workInProgress = true;

	public Object handle(Expression<?> expression) {
		return expression.accept(this, null);
	}

	// TODO
	public Sort toSort(List<OrderSpecifier<?>> orderBys) {
		Sort sort = Sort.unsorted();
		for (OrderSpecifier<?> orderBy : orderBys) {
			Object key = orderBy.getTarget().accept(this, null);
			// sort.and(Sort.by(orderBy));
			// sort.append(key.toString(), orderBy.getOrder() == Order.ASC ? 1 : -1);
		}
		return sort;
	}

	@Override
	public Object visit(Constant<?> expr, Void context) {
		if (Enum.class.isAssignableFrom(expr.getType())) {
			@SuppressWarnings("unchecked") // Guarded by previous check
			Constant<? extends Enum<?>> expectedExpr = (Constant<? extends Enum<?>>) expr;
			return expectedExpr.getConstant().name();
		} else {
			return expr.getConstant();
		}
	}

	@Override
	public Object visit(TemplateExpression<?> expr, Void context) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(FactoryExpression<?> expr, Void context) {
		throw new UnsupportedOperationException();
	}

	protected String asDBKey(Operation<?> expr, int index) {
		return (String) asDBValue(expr, index);
	}

	protected Object asDBValue(Operation<?> expr, int index) {
		return expr.getArg(index).accept(this, null);
	}

	private String regexValue(Operation<?> expr, int index) {
		return Pattern.quote(expr.getArg(index).accept(this, null).toString());
	}

	protected QueryCriteriaDefinition asDocument(String key, Object value) {
		QueryCriteria qc = null;
		if (workInProgress) {
			throw new UnsupportedOperationException("Wrong path to create this criteria " + key);
		}
		if (key.equals(Ops.AND) || key.equals(Ops.OR)) {
			throw new UnsupportedOperationException("Wrong path to create this criteria " + key);
		} else if (key.equals(Ops.IN)) {
			throw new RuntimeException(("not supported"));
		} else {
			qc = QueryCriteria.where(key).is(value);
		}

		return qc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object visit(Operation<?> expr, Void context) {
		Operator op = expr.getOperator();
		if (op == Ops.EQ) {
			if (expr.getArg(0) instanceof Operation) {
				Operation<?> lhs = (Operation<?>) expr.getArg(0);
				if (lhs.getOperator() == Ops.COL_SIZE || lhs.getOperator() == Ops.ARRAY_SIZE
						|| lhs.getOperator() == Ops.STRING_LENGTH) {
					return QueryCriteria.where(asDBKey(expr, 0)).is(asDBValue(expr, 1));
				} else {
					throw new UnsupportedOperationException("Illegal operation " + expr);
				}
			} else if (expr.getArg(0) instanceof Path) {
				return QueryCriteria.where(asDBKey(expr, 0)).is(asDBValue(expr, 1));
			}
		} else if (op == Ops.STRING_IS_EMPTY) {
			return QueryCriteria.where(asDBKey(expr, 0)).isNotValued().or(asDBKey(expr, 0)).is("");
		} else if (op == Ops.NOT) {
			// Handle the not's child
			Operation<?> subOperation = (Operation<?>) expr.getArg(0);
			Operator subOp = subOperation.getOperator();
			if (subOp == Ops.IN) {
				return visit(
						ExpressionUtils.operation(Boolean.class, Ops.NOT_IN, subOperation.getArg(0), subOperation.getArg(1)),
						context);
			} else {
				QueryCriteria arg = (QueryCriteria) handle(expr.getArg(0));
				return arg.negate();
			}

		} else if (op == Ops.AND) {
			return collectConnectorArgs(Ops.AND, expr);
		} else if (op == Ops.OR) {
			return collectConnectorArgs(Ops.OR, expr);
		} else if (op == Ops.NE) {
			return QueryCriteria.where(asDBKey(expr, 0)).ne(asDBValue(expr, 1));
		} else if (op == Ops.STARTS_WITH) {
			return QueryCriteria.where(asDBKey(expr, 0)).startingWith(asDBValue(expr, 1));
		} else if (op == Ops.STARTS_WITH_IC) {
			return QueryCriteria.where(asDBKey(expr, 0)).upper()
					.startingWith(asDBValue(expr, 1).toString().toUpperCase(Locale.ROOT));
		} else if (op == Ops.ENDS_WITH) {
			return QueryCriteria.where(asDBKey(expr, 0)).endingWith(asDBValue(expr, 1));
		} else if (op == Ops.ENDS_WITH_IC) {
			return QueryCriteria.where(asDBKey(expr, 0)).upper()
					.endingWith(asDBValue(expr, 1).toString().toUpperCase(Locale.ROOT));
		} else if (op == Ops.EQ_IGNORE_CASE) {
			return QueryCriteria.where(asDBKey(expr, 0)).upper().eq(asDBValue(expr, 1).toString().toUpperCase(Locale.ROOT));
		} else if (op == Ops.STRING_CONTAINS) {
			return QueryCriteria.where(asDBKey(expr, 0)).containing(asDBValue(expr, 1));
		} else if (op == Ops.STRING_CONTAINS_IC) {
			return QueryCriteria.where(asDBKey(expr, 0)).upper()
					.containing(asDBValue(expr, 1).toString().toUpperCase(Locale.ROOT));
		} else if (op == Ops.MATCHES) {
			return QueryCriteria.where(asDBKey(expr, 0)).regex(asDBValue(expr, 1));
		} else if (op == Ops.MATCHES_IC) {
			return QueryCriteria.where("UPPER(" + asDBKey(expr, 0) + ")").regex("UPPER(" + asDBValue(expr, 1) + ")");
		} else if (op == Ops.LIKE) {
			return QueryCriteria.where(asDBKey(expr, 0)).like(asDBValue(expr, 1));
		} else if (op == Ops.LIKE_IC) {
			return QueryCriteria.where(asDBKey(expr, 0)).upper().like(asDBValue(expr, 1).toString().toUpperCase(Locale.ROOT));
		} else if (op == Ops.BETWEEN) {
			return QueryCriteria.where(asDBKey(expr, 0)).between(asDBValue(expr, 1), asDBValue(expr, 2));
		} else if (op == Ops.IN) {
			int constIndex = 0;
			int exprIndex = 1;
			if (expr.getArg(1) instanceof Constant<?>) {
				constIndex = 1;
				exprIndex = 0;
			}
			if (Collection.class.isAssignableFrom(expr.getArg(constIndex).getType())) {
				@SuppressWarnings("unchecked") // guarded by previous check
				Collection<?> values = ((Constant<? extends Collection<?>>) expr.getArg(constIndex)).getConstant();
				return QueryCriteria.where(asDBKey(expr, exprIndex)).in(values);
			} else { // framework already converts IN to EQ if arg is not a collection
				if (workInProgress)
					throw new RuntimeException("good, we are testing in -> eq");
				Object value = expr.getArg(constIndex);
				return QueryCriteria.where(asDBKey(expr, exprIndex)).eq(value);
			}
		} else if (op == Ops.NOT_IN) {
			int constIndex = 0;
			int exprIndex = 1;
			if (expr.getArg(1) instanceof Constant<?>) {
				constIndex = 1;
				exprIndex = 0;
			}
			if (Collection.class.isAssignableFrom(expr.getArg(constIndex).getType())) {
				@SuppressWarnings("unchecked") // guarded by previous check
				Collection<?> values = ((Constant<? extends Collection<?>>) expr.getArg(constIndex)).getConstant();
				return QueryCriteria.where(asDBKey(expr, exprIndex)).notIn(values);
			} else { // framework already converts NOT_IN to NE if arg is not a collection
				if (workInProgress)
					throw new RuntimeException("good, we are testing not_in -> ne");
				Object value = expr.getArg(constIndex);
				return QueryCriteria.where(asDBKey(expr, exprIndex)).ne(value);
			}
		} else if (op == Ops.COL_IS_EMPTY) {
			return QueryCriteria.where(asDBKey(expr, 0)).isNotValued();
		} else if (op == Ops.LT) {
			return QueryCriteria.where(asDBKey(expr, 0)).lt(asDBValue(expr, 1));
		} else if (op == Ops.GT) {
			return QueryCriteria.where(asDBKey(expr, 0)).gt(asDBValue(expr, 1));
		} else if (op == Ops.LOE) {
			return QueryCriteria.where(asDBKey(expr, 0)).lte(asDBValue(expr, 1));
		} else if (op == Ops.GOE) {
			return QueryCriteria.where(asDBKey(expr, 0)).gte(asDBValue(expr, 1));
		} else if (op == Ops.IS_NULL) {
			return QueryCriteria.where(asDBKey(expr, 0)).isNull();
		} else if (op == Ops.IS_NOT_NULL) {
			return QueryCriteria.where(asDBKey(expr, 0)).isNotNull();
		} else if (op == Ops.CONTAINS_KEY) { // TODO not sure about this one
			return QueryCriteria.where("meta().id"/*asDBKey(expr, 0)*/).eq(asDBKey(expr, 1));
		} else if (op == Ops.STRING_LENGTH) {
			return "LENGTH(" + asDBKey(expr, 0) + ")";// QueryCriteria.where(asDBKey(expr, 0)).size();
		}
		throw new UnsupportedOperationException("Unsupported operation " + expr);
	}

	protected Object convert(Path<?> property, Constant<?> constant) {
		if (isReference(property)) {
			return asReference(constant.getConstant());
		} else if (isId(property)) {
			if (isReference(property.getMetadata().getParent())) {
				return asReferenceKey(property.getMetadata().getParent().getType(), constant.getConstant());
			} else if (constant.getType().equals(String.class) && isImplicitObjectIdConversion()) {
				String id = (String) constant.getConstant();
				return id;
			}
		}
		return visit(constant, null);
	}

	protected boolean isImplicitObjectIdConversion() {
		return true;
	}

	protected DBRef asReferenceKey(Class<?> entity, Object id) {
		// TODO override in subclass
		throw new UnsupportedOperationException();
	}

	protected abstract DBRef asReference(Object constant);

	protected abstract boolean isReference(Path<?> arg);

	protected boolean isId(Path<?> arg) {
		// TODO override in subclass
		return false;
	}

	@Override
	public String visit(Path<?> expr, Void context) {
		PathMetadata metadata = expr.getMetadata();
		if (metadata.getParent() != null) {
			Path<?> parent = metadata.getParent();
			if (parent.getMetadata().getPathType() == PathType.DELEGATE) {
				parent = parent.getMetadata().getParent();
			}
			if (metadata.getPathType() == PathType.COLLECTION_ANY) {
				return visit(parent, context);
			} else if (parent.getMetadata().getPathType() != PathType.VARIABLE) {
				String rv = getKeyForPath(expr, metadata);
				String parentStr = visit(parent, context);
				return rv != null ? parentStr + "." + rv : parentStr;
			}
		}
		return getKeyForPath(expr, metadata);
	}

	protected String getKeyForPath(Path<?> expr, PathMetadata metadata) {
		return metadata.getElement().toString();
	}

	@Override
	public Object visit(SubQueryExpression<?> expr, Void context) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object visit(ParamExpression<?> expr, Void context) {
		throw new UnsupportedOperationException();
	}

	private QueryCriteriaDefinition collectConnectorArgs(Ops operator, Operation<?> operation) {
		QueryCriteria first = null;
		for (Expression<?> exp : operation.getArgs()) {
			QueryCriteria document = (QueryCriteria) handle(exp);
			if (first == null) {
				first = document;
			} else {
				if (operator.equals(Ops.OR)) {
					first = first.or(document);
				} else if (operator.equals(Ops.AND)) {
					first = first.and(document);
				}
			}
		}
		return first;
	}
}
