namespace Confluent.Kafka.Dataflow.Internal
{
    using System.Collections.Concurrent;
    using System.Threading.Tasks;

    class ClientState<T>
    {
        static readonly ConcurrentDictionary<string, ClientState<T>> handles =
            new ConcurrentDictionary<string, ClientState<T>>();

        // Note: Different instances can share an underlying client (identified by the name).
        public static ClientState<T> Get(IClient client) =>
            handles.GetOrAdd(client.Name, new ClientState<T>());

        public TaskCompletionSource<byte> CompletionSource { get; } = new TaskCompletionSource<byte>();

        public T? State { get; set; }
    }
}
