using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
	static void Main(string[] args)
	{
		// Kafka consumer configuration
		var config = new ConsumerConfig
		{
			BootstrapServers = "localhost:9092", // Replace with your Kafka broker address
			GroupId = "topic1_CG2",    // Replace with your consumer group ID
			AutoOffsetReset = AutoOffsetReset.Earliest, // Start reading from the earliest message
			EnableAutoCommit = true             // Automatically commit offsets
		};

		// Topic name
		string topic = "wikimedia-recentchange"; // Replace with your Kafka topic name

		// Create a consumer
		using (var consumer = new ConsumerBuilder<string, string>(config).Build())
		{
			// Subscribe to the topic
			consumer.Subscribe(topic);

			Console.WriteLine($"Subscribed to topic: {topic}");
			Console.WriteLine("Waiting for messages...");

			try
			{
				while (true)
				{
					// Consume a message
					var cr = consumer.Consume(CancellationToken.None);
					// Print the message key, value, and headers
					Console.WriteLine($"New Message: Key = {cr.Message.Key}, Message = {cr.Message.Value} Partition : {cr.Partition} at Offset:{cr.Offset} ");
					
				}
			}
			catch (OperationCanceledException)
			{
				Console.WriteLine("Consumption canceled.");
			}
			finally
			{
				// Ensure the consumer leaves the group cleanly
				consumer.Close();
			}
		}
	}
}