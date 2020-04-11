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
package io.kcache.utils.rocksdb;

import io.kcache.Transaction;
import io.kcache.exceptions.CacheException;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBTransaction implements Transaction {

    private static final Logger log = LoggerFactory.getLogger(RocksDBTransaction.class);

    private org.rocksdb.Transaction tx;

    public RocksDBTransaction(org.rocksdb.Transaction tx) {
        this.tx = tx;
    }

    public org.rocksdb.Transaction getTx() {
        return tx;
    }

    public void commit() throws CacheException {
        try {
            tx.commit();
        } catch (RocksDBException e) {
            throw new CacheException("Could not commit tx", e);
        }
    }

    public void rollback() throws CacheException {
        try {
            tx.rollback();
        } catch (RocksDBException e) {
            throw new CacheException("Could not rollback tx", e);
        }
    }
}
