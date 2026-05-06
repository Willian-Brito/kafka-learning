using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;

var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "KafkaLearning"    
};

using var consumer = new ConsumerBuilder<string, kafkaLearning.Curso>(config)
    .SetValueDeserializer(new AvroDeserializer<kafkaLearning.Curso>(schemaRegistry).AsSyncOverAsync())
    .Build();

var topic = "cursos";
consumer.Subscribe(topic);

while(true)
{
    var result = consumer.Consume();
    Console.WriteLine($"Mensagem recebida: {result.Message.Value.Descricao}");
}