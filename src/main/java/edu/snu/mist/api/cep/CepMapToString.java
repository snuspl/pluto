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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class for Translate input Map into String.
 */
public final class CepMapToString implements MISTFunction<Map<String, Object>, String> {
    private final List<Object> fields;
    private final String separator;

    public CepMapToString(final List<Object> fieldsParam, final String separatorParam) {
        this.fields = fieldsParam;
        this.separator = separatorParam;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CepMapToString that = (CepMapToString) o;

        if (fields != null ? !fields.equals(that.fields) : that.fields != null) {
            return false;
        }
        return separator != null ? separator.equals(that.separator) : that.separator == null;
    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + (separator != null ? separator.hashCode() : 0);
        return result;
    }

    @Override
    public String apply(final Map<String, Object> s) {
        final StringBuilder strBuilder = new StringBuilder();

        for (final Object iter : fields) {
            strBuilder.append(iter.toString());
            strBuilder.append(separator);
        }

        if (strBuilder.length() == 0) {
            throw new NullPointerException("No Parameters for cepSink!");
        }

        strBuilder.delete(strBuilder.length() - separator.length(), strBuilder.length());

        final Iterator<String> iter = s.keySet().iterator();
        String resultStr = strBuilder.toString();
        while (iter.hasNext()) {
            final String field = iter.next();
            if (resultStr.matches(".*" + "[$]" + field + ".*")) {
                resultStr = resultStr.replaceAll("[$]" + field, s.get(field).toString());
            }
        }
        return resultStr;
    }
}
