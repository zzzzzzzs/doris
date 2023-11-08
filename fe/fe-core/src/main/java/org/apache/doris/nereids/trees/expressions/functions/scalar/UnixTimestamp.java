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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullableOnDateLikeV2Args;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'unix_timestamp'. This class is generated by GenerateFunction.
 */
public class UnixTimestamp extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullableOnDateLikeV2Args, Nondeterministic {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(IntegerType.INSTANCE).args(),
            FunctionSignature.ret(IntegerType.INSTANCE).args(DateTimeV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(IntegerType.INSTANCE).args(DateV2Type.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(DateTimeType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(DateType.INSTANCE),
            FunctionSignature.ret(IntegerType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(IntegerType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE)
    );

    /**
     * constructor with 0 argument.
     */
    public UnixTimestamp() {
        super("unix_timestamp");
    }

    /**
     * constructor with 1 argument.
     */
    public UnixTimestamp(Expression arg) {
        super("unix_timestamp", arg);
    }

    /**
     * constructor with 2 arguments.
     */
    public UnixTimestamp(Expression arg0, Expression arg1) {
        super("unix_timestamp", arg0, arg1);
    }

    /**
     * custom compute nullable.
     */
    @Override
    public boolean nullable() {
        if (arity() == 0) {
            return false;
        }
        return PropagateNullableOnDateLikeV2Args.super.nullable();
    }

    /**
     * withChildren.
     */
    @Override
    public UnixTimestamp withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 0
                || children.size() == 1
                || children.size() == 2);
        if (children.isEmpty() && arity() == 0) {
            return this;
        } else if (children.size() == 1) {
            return new UnixTimestamp(children.get(0));
        } else {
            return new UnixTimestamp(children.get(0), children.get(1));
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnixTimestamp(this, context);
    }
}
