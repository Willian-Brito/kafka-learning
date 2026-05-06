using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

var schemaConfig = new SchemaRegistryConfig
{
    Url = "http://localhost:8081"
};

var schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

var config = new ProducerConfig { 
    BootstrapServers = "localhost:9092" 
};

using var producer = new ProducerBuilder<string, kafkaLearning.Curso>(config)
    .SetValueSerializer(new AvroSerializer<kafkaLearning.Curso>(schemaRegistry))
    .Build();

var message = new Message<string, kafkaLearning.Curso>
{
    Key = Guid.NewGuid().ToString(),
    Value = new kafkaLearning.Curso
    {
        Id = Guid.NewGuid().ToString(),
        Descricao = "Curso de Apache Kafka"
    }
};

var topic = "cursos";
var result = await producer.ProduceAsync(topic, message);

Console.WriteLine($"{result.Offset}");