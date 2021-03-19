namespace Confluent.Kafka.Dataflow.Internal
{
    using System.Collections.Concurrent;

    class ClientCache<T>
        where T : new()
    {
        readonly ConcurrentDictionary<string, T> cache = new();

        // Note: Different instances can share an underlying client (identified by the name).
        public T GetOrAdd(IClient client) => cache.GetOrAdd(client.Name, _ => new T());
    }
}
