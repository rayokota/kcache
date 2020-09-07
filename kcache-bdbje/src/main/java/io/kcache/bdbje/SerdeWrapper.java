/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache.bdbje;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class SerdeWrapper<T> implements Serde<T>, Serializable {
    private static final long serialVersionUID = -302623791968470800L;

    private static final Kryo kryo = new Kryo();

    static {
        kryo.setRegistrationRequired(false);
    }

    private Serde<T> serde;

    public SerdeWrapper(Serde<T> serde) {
        this.serde = serde;
    }

    @Override
    public Serializer<T> serializer() {
        return serde.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return serde.deserializer();
    }

    private void writeObject(java.io.ObjectOutputStream stream)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(32);
        Output output = new Output(baos);
        kryo.writeClassAndObject(output, serde);
        output.close();
        stream.write(baos.toByteArray());
    }

    @SuppressWarnings("unchecked")
    private void readObject(java.io.ObjectInputStream stream)
        throws IOException, ClassNotFoundException {
        Input input = new Input(stream);
        serde = (Serde<T>) kryo.readClassAndObject(input);
    }
}
