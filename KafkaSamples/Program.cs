using System;
using System.Net.Http;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Text.Json;

class Program
{
	private const string WikimediaStreamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";
	private const string KafkaBootstrapServers = "localhost:9092";  // Change to your Kafka broker
	private const string KafkaTopic = "wikimedia-recentchange";

	static async Task Main()
	{
		var config = new ProducerConfig { BootstrapServers = KafkaBootstrapServers };
		using var producer = new ProducerBuilder<string, string>(config).Build();

		using var client = new HttpClient();
		using var stream = await client.GetStreamAsync(WikimediaStreamUrl);
		using var reader = new System.IO.StreamReader(stream);

		Console.WriteLine("Listening to Wikimedia recent changes stream...");

		while (!reader.EndOfStream)
		{
			var line = await reader.ReadLineAsync();
			if (string.IsNullOrWhiteSpace(line) || !line.StartsWith("data:"))
				continue;

			var jsonData = line.Substring(5).Trim(); // Remove "data:" prefix

			try
			{
				var changeEvent = JsonSerializer.Deserialize<JsonElement>(jsonData);
				string key = changeEvent.GetProperty("id").ToString();
				await producer.ProduceAsync(KafkaTopic, new Message<string, string> { Key = key, Value = jsonData });

				Console.WriteLine($"Published event ID: {key}");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Error processing event: {ex.Message}");
			}
		}
	}
}
