using System.Collections.Concurrent;
using System.Text;
using RabbitMQ.Client;

var factory = new ConnectionFactory { HostName = "localhost", AutomaticRecoveryEnabled = true };
using var connection = factory.CreateConnection();
connection.ConnectionShutdown += (conn, reason) => Console.WriteLine("Connection shut down because of: " + reason);

using var channel = connection.CreateModel();

ulong count = 0;
channel.QueueDeclare(queue: "hello",
                     durable: true,
                     exclusive: false,
                     autoDelete: false,
                     arguments: null /* TODO: Quorum */);

// Configure to use publisher confirms:
channel.ConfirmSelect();
var outstandingConfirms = new ConcurrentQueue<ulong>();
channel.BasicAcks += (sender, ea) => CleanOutstandingConfirms(outstandingConfirms, ea.DeliveryTag, ea.Multiple);
channel.BasicNacks += (sender, ea) => CleanOutstandingConfirms(outstandingConfirms, ea.DeliveryTag, ea.Multiple);

ConsoleKeyInfo key;
do {
    string message = $"Hello {++count}!";
    var body = Encoding.UTF8.GetBytes(message);

    var sequenceNumber = channel.NextPublishSeqNo;
    outstandingConfirms.Enqueue(sequenceNumber);
    channel.BasicPublish(exchange: string.Empty,
                     routingKey: "hello",
                     basicProperties: null,
                     body: body);

    // This is very inefficient:              
    //channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));

    Console.WriteLine($" [x] Sent: {message}");
    Console.WriteLine(" Press [enter] to exit or any other key to continue.");
    key = Console.ReadKey();
    
} while (key.Key != ConsoleKey.Enter);

void CleanOutstandingConfirms(ConcurrentQueue<ulong> outstandingConfirms, ulong sequenceNumber, bool multiple)
{
    if (multiple)
    {
        for (;;) 
        {
            outstandingConfirms.TryPeek(out var tag);
            if (tag > sequenceNumber)
                break;
            else
                outstandingConfirms.TryDequeue(out _);
        }
    }
    else
    {
        outstandingConfirms.TryDequeue(out _);
    }
    Console.WriteLine("Awaiting confirms: {0} messages (multiple: {1})", outstandingConfirms.Count, multiple);
}


