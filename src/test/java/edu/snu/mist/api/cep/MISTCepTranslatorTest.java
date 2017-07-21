/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.api.cep;

import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.cep.conditions.ComparisonCondition;
import edu.snu.mist.api.cep.conditions.ConditionType;
import edu.snu.mist.api.cep.conditions.UnionCondition;
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.common.types.Tuple2;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The test for translating CEP query into MIST query.
 */
public class MISTCepTranslatorTest {
    private final String cepInputAddress = "some.inputaddress.com";
    private final Integer cepInputPort = 8080;
    private final String firstFieldName = "Location";
    private final String secondFieldName = "Temperature";
    private final CepValueType firstFieldType = CepValueType.STRING;
    private final CepValueType secondFieldType = CepValueType.DOUBLE;
    private final String inputSeparator = ",";

    private final String  cepOutputAddress = "some.outputaddress.com";
    private final CepActionType cepActionType = CepActionType.TEXT_WRITE;
    private final Integer cepOutputPort = 8088;
    private final String outputSeparator = ".";

    private final String groupId = "test-group";
    private final String compareObject1 = "Seoul";
    private final Double compareObject2 = -10.0;
    private final Double compareObject3 = 35.0;
    private final String param = "Temperature($" + secondFieldName + ") in $" + firstFieldName + " is awkward!";

    private final CepInput exampleCepInput = new CepInput.TextSocketBuilder()
            .setSocketAddress(cepInputAddress)
            .setSocketPort(cepInputPort)
            .setSeparator(inputSeparator)
            .addField(firstFieldName, firstFieldType)
            .addField(secondFieldName, secondFieldType)
            .build();

    private final CepSink exampleCepSink = new CepSink.TextSocketBuilder()
            .setSocketAddress(cepOutputAddress)
            .setSocketPort(cepOutputPort)
            .setSeparator(outputSeparator)
            .build();

    @Test
    public void testCepStatelessTranslator() throws IOException, InjectionException {
        //Make CEP query.
        final MISTCepStatelessQuery exampleCepQuery = new MISTCepStatelessQuery.Builder(groupId)
                .input(exampleCepInput)
                .addStatelessRule(new CepStatelessRule.Builder()
                        .setCondition(
                                UnionCondition.and(
                                    ComparisonCondition.eq(firstFieldName, compareObject1),
                                    UnionCondition.or(
                                            ComparisonCondition.lt(secondFieldName, compareObject2),
                                            ComparisonCondition.gt(secondFieldName, compareObject3)
                                    )
                                )
                        )
                        .setAction(new CepAction.Builder()
                                .setActionType(cepActionType)
                                .setCepSink(exampleCepSink)
                                .setParams(param)
                                .build())
                        .build()
                )
                .build();

        //Translate CEP query into MIST query.
        final MISTQuery exampleMistQuery = CepTranslator.cepStatelessTranslator(exampleCepQuery);

        //Instances for making MIST query.
        final SourceConfiguration exampleMistConf = TextSocketSourceConfiguration.newBuilder()
                .setHostAddress(cepInputAddress)
                .setHostPort(cepInputPort)
                .build();
        final List<Tuple2<String, CepValueType>> exampleFields = new ArrayList<>();
        exampleFields.add(new Tuple2<>(firstFieldName, firstFieldType));
        exampleFields.add(new Tuple2<>(secondFieldName, secondFieldType));
        final List<Object> exampleList = new ArrayList<>();
        exampleList.add(param);

        //Make MIST query with data flow API.
        final MISTQueryBuilder exampleMistQueryBuilder = new MISTQueryBuilder(groupId);
        final ContinuousStream<Map<String, Object>> temp1 = exampleMistQueryBuilder.socketTextStream(exampleMistConf)
                .map(new CepStringToMap(exampleFields, inputSeparator))
                .filter(new CepCCPredicate(ConditionType.EQ, firstFieldName, compareObject1));
        final ContinuousStream<Map<String, Object>> temp2 = temp1
                .filter(new CepCCPredicate(ConditionType.LT, secondFieldName, compareObject2));
        final ContinuousStream<Map<String, Object>> temp3 = temp1
                .filter(new CepCCPredicate(ConditionType.GT, secondFieldName, compareObject3));
        temp2.union(temp3)
                .map(new CepMapToString(exampleList, outputSeparator))
                .textSocketOutput(cepOutputAddress, cepOutputPort);
        final MISTQuery expectedMistQuery = exampleMistQueryBuilder.build();

        //Compare two MIST queries.
        Assert.assertEquals(expectedMistQuery.getGroupId(), exampleCepQuery.getGroupId());
        Assert.assertEquals(expectedMistQuery.getDAG().numberOfVertices(),
                exampleMistQuery.getDAG().numberOfVertices());
        System.out.println(expectedMistQuery.getDAG().numberOfVertices());
        Assert.assertEquals(expectedMistQuery.getDAG().numberOfEdges(),
                exampleMistQuery.getDAG().numberOfEdges());
        Assert.assertEquals(expectedMistQuery.getAvroOperatorChainDag(), exampleMistQuery.getAvroOperatorChainDag());
    }
}
