using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost", AutomaticRecoveryEnabled = true };
using var connection = factory.CreateConnection();
connection.ConnectionShutdown += (conn, reason) => Console.WriteLine("Connection shut down because of: " + reason);

using var channel = connection.CreateModel();

channel.QueueDeclare(queue: "hello",
                     durable: false,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null);

const string message = "Hello World!";
var body = Encoding.UTF8.GetBytes(message);

ConsoleKeyInfo key;
do {
    channel.BasicPublish(exchange: string.Empty,
                     routingKey: "hello",
                     basicProperties: null,
                     body: body);

    Console.WriteLine($" [x] Sent {message}");
    Console.WriteLine(" Press [enter] to exit or any other key to continue.");
    key = Console.ReadKey();
    
} while (key.Key != ConsoleKey.Enter);


