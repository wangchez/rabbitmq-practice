using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;
using System.Threading;
using RabbitMQ.Client.MessagePatterns;

namespace RabbitMQSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var cancellationSource = new CancellationTokenSource();
            var cancellationToken = cancellationSource.Token;
            cancellationToken.Register(() => Console.WriteLine("Job cancel"));

            var jobCompleteSource = new TaskCompletionSource<bool>();

            var factory = new ConnectionFactory() { HostName = "localhost" };
                 
            using (var connection = factory.CreateConnection())
            {
                                                                                                   
                
                Console.WriteLine($"max channel{connection.ChannelMax}");
                cancellationSource.CancelAfter(2000);
                try
                {
                    var listeningChannel1 = Task.Run(() => CreateChannel(connection, cancellationToken), cancellationToken);
                    var listeningChannel2 = Task.Run(() => CreateChannel(connection, cancellationToken), cancellationToken);

                    Console.WriteLine("listening...");
                    Console.ReadLine();
                }
                catch
                {
                }
            }

            Console.WriteLine("exit.");
            Console.ReadLine();
        }

        private static void CreateChannel(IConnection connection, CancellationToken token)
        {
            using (var channel = connection.CreateModel())
            {
                channel.BasicQos(0, 1, false);
                Console.WriteLine($"channel:{channel.ChannelNumber}");

                channel.ModelShutdown += ChannelModelShutdown;                
                channel.QueueDeclare(queue: "hello", durable: false, exclusive: false, autoDelete: false, arguments: null);
                
                var sub = new Subscription(channel, "hello", false);
                sub.Consumer.ConsumerCancelled += ConsumerCancelled;
                token.Register(() => channel.BasicCancel(sub.ConsumerTag));
                foreach (BasicDeliverEventArgs e in sub)
                {
                    Console.WriteLine($"consumer:{sub.ConsumerTag} start working...");
                    var message = Encoding.UTF8.GetString(e.Body);
                    Task.Delay(4000).ContinueWith(t => Console.WriteLine("complete a long run task")).Wait();
                    Console.WriteLine(" [x] Received {0} by channel {1}", message, channel.ChannelNumber);
                    sub.Ack(e);
                }
            }
        }

        private static void ConsumerCancelled(object sender, ConsumerEventArgs e)
        {
            Console.WriteLine($"consumer {e.ConsumerTag} cancelled");
        }

        private static void ChannelModelShutdown(object sender, ShutdownEventArgs e)
        {
            Console.WriteLine($"channel down {e.ReplyText}");
        }
    }
}
