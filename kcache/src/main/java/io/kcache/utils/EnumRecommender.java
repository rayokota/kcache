/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class EnumRecommender<T extends Enum> implements ConfigDef.Validator, ConfigDef.Recommender {

    private final Set<String> validValues;
    private final Class<T> enumClass;

    public EnumRecommender(
        Class<T> enumClass,
        Function<String, String> conversion,
        T... excludedValues
    ) {
        this.enumClass = enumClass;
        Set<String> validEnums = new LinkedHashSet<>();
        for (Object o : enumClass.getEnumConstants()) {
            String key = conversion.apply(o.toString());
            validEnums.add(key);
        }
        for (Object excluded : excludedValues) {
            String key = conversion.apply(excluded.toString());
            validEnums.remove(key);
        }
        this.validValues = ImmutableSet.copyOf(validEnums);
    }

    @Override
    public void ensureValid(String key, Object value) {
        // calling toString on itself because IDE complains if the Object is passed.
        if (value != null && !validValues.contains(value.toString())) {
            throw new ConfigException(key, value, "Invalid enumerator");
        }
    }

    @Override
    public String toString() {
        return validValues.toString();
    }

    @Override
    public List<Object> validValues(String name, Map<String, Object> connectorConfigs) {
        return ImmutableList.copyOf(validValues);
    }

    @Override
    public boolean visible(String name, Map<String, Object> connectorConfigs) {
        return true;
    }
}
