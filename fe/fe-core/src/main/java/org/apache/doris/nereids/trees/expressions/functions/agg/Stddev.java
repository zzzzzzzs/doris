// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.DecimalStddevPrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AggregateFunction 'stddev'. This class is generated by GenerateFunction.
 */
public class Stddev extends NullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature,
        StdDevOrVarianceFunction, DecimalStddevPrecision {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(DoubleType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(IntegerType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(BigIntType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(FloatType.INSTANCE),
            FunctionSignature.ret(DecimalV2Type.SYSTEM_DEFAULT).args(DecimalV2Type.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 1 argument.
     */
    public Stddev(Expression arg) {
        this(false, false, arg);
    }

    /**
     * constructor with 1 argument.
     */
    public Stddev(boolean distinct, Expression arg) {
        this(distinct, false, arg);
    }

    private Stddev(boolean distinct, boolean alwaysNullable, Expression arg) {
        super("stddev", distinct, alwaysNullable, arg);
    }

    /**
     * withDistinctAndChildren.
     */
    @Override
    public Stddev withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Stddev(distinct, alwaysNullable, children.get(0));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new Stddev(distinct, alwaysNullable, children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStddev(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
