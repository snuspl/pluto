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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.parameters.EventSequence;
import edu.snu.mist.common.parameters.WindowTime;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

import java.util.List;

/**
 * A configuration for the operators that use SequenceScanOperator.
 */
public final class SequenceScanOperatorConfiguration extends ConfigurationModuleBuilder {

    /**
     * Required event sequence.
     */
    public static final RequiredParameter<List> EVENT_SEQUENCE = new RequiredParameter<>();

    /**
     * Required event window time.
     */
    public static final RequiredParameter<Long> WINDOW_TIME = new RequiredParameter<>();

    /**
     * Required operator class.
     */
    public static final RequiredImpl<Operator> OPERATOR = new RequiredImpl<>();

    public static final ConfigurationModule CONF = new SequenceScanOperatorConfiguration()
        .bindList(EventSequence.class, EVENT_SEQUENCE)
        .bindNamedParameter(WindowTime.class, WINDOW_TIME)
        .bindImplementation(Operator.class, OPERATOR)
        .build();
}
