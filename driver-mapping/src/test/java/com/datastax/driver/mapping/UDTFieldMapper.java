package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TestUtils;
import com.datastax.driver.mapping.annotations.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

public class UDTFieldMapper {

    /**
     * Test that
     */
    @Test
    public void stuff() {
        TestUtils.versionCheck(2.1, 0, "This will only work with C* 2.1.0");

        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            // Create type and table
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
            Session session = cluster.connect();
            session.execute("create schema if not exists java_509 " +
                    "with replication= { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
            session.execute("use java_509");
            session.execute("create type my_tuple (type text, value text);");
            session.execute("create table my_hash(key int primary key, properties map<text, frozen<my_tuple>>);");
            cluster.close();

            // Create entities with another connection
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
            Mapper<MyHash> hashMapper = new MappingManager(cluster.newSession()).mapper(MyHash.class);
            hashMapper.save(new MyHash(
                    1,
                    HashMapHelper.from("key-1", new MyTuple("first-half-1", "second-half-1"))));
            hashMapper.save(new MyHash(
                    2,
                    HashMapHelper.from("key-2", new MyTuple("first-half-2", null))));
            hashMapper.save(new MyHash(
                    3,
                    HashMapHelper.from("key-3", new MyTuple(null, "second-half-3"))));
            hashMapper.save(new MyHash(
                    4,
                    new HashMap<String, MyTuple>()));
            hashMapper.save(new MyHash(
                    5,
                    null));
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @UDT(name = "my_tuple")
    static class MyTuple {
        @Field(name = "type")
        private String type;
        @Field(name = "value")
        private String value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Table(name = "my_hash")
    static class MyHash {
        @PartitionKey
        @Column(name = "key")
        private int key;

        @FrozenValue
        @Column(name = "properties")
        private Map<String, MyTuple> properties;
    }

    static class HashMapHelper {
        public static <K, V> HashMap<K, V> from(K k, V v) {
            HashMap<K, V> m = new HashMap<K, V>();
            m.put(k, v);
            return m;
        }
    }
}
