namespace Confluent.Kafka.Dataflow
{
    using System;
    using System.Threading.Tasks.Dataflow;
    using Confluent.Kafka.Dataflow.Internal;

    /// <summary>
    /// Extensions to represent Kafka producers and consumers as dataflow blocks.
    /// </summary>
    public static class ClientExtensions
    {
        /// <summary>
        /// Represents a consumer as a source block for Kafka messages.
        /// </summary>
        /// <remarks>
        /// Consumer must be subscribed/assigned for messages to be received.
        /// </remarks>
        /// <typeparam name="TKey">The consumer key type.</typeparam>
        /// <typeparam name="TValue">The consumer value type.</typeparam>
        /// <param name="consumer">The consumer.</param>
        /// <param name="options">Block options for consuming.</param>
        /// <returns>The consumer source block.</returns>
        public static ISourceBlock<Message<TKey, TValue>> AsSourceBlock<TKey, TValue>(
            this IConsumer<TKey, TValue> consumer,
            ConsumeBlockOptions? options = null)
        {
            return new ConsumeBlock<TKey, TValue>(
                consumer ?? throw new ArgumentNullException(nameof(consumer)),
                options ?? new());
        }
    }
}
