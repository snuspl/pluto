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

import edu.snu.mist.common.types.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import static edu.snu.mist.api.cep.CepValueType.*;

/**
 * Class for using Tuple in Cep.
 */
public final class CepTuple implements Serializable{

    /**
     * Translate input String into HashMap.
     * @param input input string
     * @param fields properties given by builder
     * @param separator cep input separator
     * @return hash map of field name and its value
     */
    public static HashMap<String, Tuple2<Object, CepValueType>> stringToMap(
            final String input,
            final List<Tuple2<String, CepValueType>> fields,
            final String separator) {
        final String[] inputParse = input.split(separator);
        int inputSize = inputParse.length;

        if(inputSize < fields.size()){
            throw new IllegalStateException("Cannot match input string to tuple since the size is different!");
        }

        final HashMap<String, Tuple2<Object, CepValueType>> result = new HashMap<>();

        Object value;

        //if inputSize is larger than fields size, then the spare parts are eliminated.
        for(int i=0; i< fields.size(); i++) {
            Tuple2<String, CepValueType> tuple = fields.get(i);
            switch((CepValueType)tuple.get(1)){
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
            result.put((String)tuple.get(0), new Tuple2(value, tuple.get(1)));
        }

        return result;
    }

    private CepTuple(){}
}
