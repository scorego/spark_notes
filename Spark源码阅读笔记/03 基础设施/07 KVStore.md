

https://issues.apache.org/jira/browse/SPARK-18085

```java
package org.apache.spark.util.kvstore;

/**
 * Abstraction for a local key/value store for storing app data.
 *
 * There are two main features provided by the implementations of this interface:
 *
 * 1. Serialization
 * If the underlying data store requires serialization, data will be serialized to and deserialized
 * using a {@link KVStoreSerializer}, which can be customized by the application. The serializer is
 * based on Jackson, so it supports all the Jackson annotations for controlling the serialization of
 * app-defined types.
 * Data is also automatically compressed to save disk space.
 *
 * 2. Automatic Key Management
 *
 * When using the built-in key management, the implementation will automatically create unique
 * keys for each type written to the store. Keys are based on the type name, and always start
 * with the "+" prefix character (so that it's easy to use both manual and automatic key
 * management APIs without conflicts).
 * Another feature of automatic key management is indexing; by annotating fields or methods of
 * objects written to the store with {@link KVIndex}, indices are created to sort the data
 * by the values of those properties. This makes it possible to provide sorting without having
 * to load all instances of those types from the store.
 * 
 * KVStore instances are thread-safe for both reads and writes.
 */
@Private
public interface KVStore extends Closeable {

    // Returns app-specific metadata from the store, or null if it's not currently set.
    <T> T getMetadata(Class<T> klass) throws Exception;

    // Writes the given value in the store metadata key.
    void setMetadata(Object value) throws Exception;

    // Read a specific instance of an object.
    <T> T read(Class<T> klass, Object naturalKey) throws Exception;

    // Writes the given object to the store, including indexed fields. Indices are updated based
    // on the annotated fields of the object's class.
    void write(Object value) throws Exception;

    // Removes an object and all data related to it, like index entries, from the store.
    void delete(Class<?> type, Object naturalKey) throws Exception;

    // Returns a configurable view for iterating over entities of the given type.
    <T> KVStoreView<T> view(Class<T> type) throws Exception;

    // Returns the number of items of the given type currently in the store.
    long count(Class<?> type) throws Exception;

    // Returns the number of items of the given type which match the given indexed value.
    long count(Class<?> type, String index, Object indexedValue) throws Exception;

    // A cheaper way to remove multiple items from the KVStore
    <T> boolean removeAllByIndexValues(Class<T> klass, String index, Collection<?> indexValues) throws Exception;
}
```

