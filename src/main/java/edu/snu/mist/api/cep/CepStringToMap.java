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

import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.types.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for Translate input String into Map.
 */
public final class CepStringToMap implements MISTFunction<String, Map<String, Object>> {
    private final List<Tuple2<String, CepValueType>> fields;
    private final String separator;

    public CepStringToMap(final List<Tuple2<String, CepValueType>> fieldsParam, final String separatorParam) {
        this.fields = fieldsParam;
        this.separator = separatorParam;
    }

    @Override
    public Map<String, Object> apply(final String s) {
        final String[] inputParse = s.split(separator);
        final int inputSize = inputParse.length;

        if (inputSize < fields.size()) {
            throw new IllegalStateException("Cannot match input string to tuple since the size is different!");
        }

        final Map<String, Object> result = new HashMap<>();
        Object value;

        //if inputSize is larger than fields size, then the spare parts are eliminated.
        for (int i = 0; i < fields.size(); i++) {
            final Tuple2<String, CepValueType> tuple = fields.get(i);
            switch ((CepValueType)tuple.get(1)) {
                case DOUBLE:
                    value = Double.parseDouble(inputParse[i].trim());
                    break;
                case INTEGER:
                    value = Integer.parseInt(inputParse[i].trim());
                    break;
                case LONG:
                    value = Long.parseLong(inputParse[i].trim());
                    break;
                case STRING:
                    value = inputParse[i].trim();
                    break;
                default:
                    throw new IllegalStateException("Fields value type is wrong!");
            }
            result.put((String)tuple.get(0), value);
        }

        return result;
    }
}
