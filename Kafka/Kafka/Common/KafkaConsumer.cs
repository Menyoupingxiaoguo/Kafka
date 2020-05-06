/*----------------------------------------------------------------
* 项目名称 ：Kafka.Common
* 项目描述 ：
* 类 名 称 ：KafkaConsumer
* 类 描 述 ：
* 所在的域 ：YANGKANG-PC
* 命名空间 ：Kafka.Common
* 机器名称 ：YANGKANG-PC 
* 作    者 ：Administrator
* 创建时间 ：2019/8/6 14:04:29
* 更新时间 ：2019/8/6 14:04:29
* 版 本 号 ：v1.0.0.0
*******************************************************************
* Copyright @ Administrator 2019. All rights reserved.
*******************************************************************/

using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Common
{
    public class KafkaConsumer
    {
        public static string brokerUrl = ConfigurationManager.AppSettings["Broker"];
        public static string topic = ConfigurationManager.AppSettings["ConsumeTopic"];
        public static string groupid = ConfigurationManager.AppSettings["GroupID"];
        public static string consumercount = ConfigurationManager.AppSettings["ConsumerCount"];
        public static void Consume()
        {
            var mode = "consume";
            var brokerList = brokerUrl;
            List<string> topics = new List<string>(topic.Split(','));

            Console.WriteLine(DateTime.Now.ToString() + " 开始接收消息队列信息！");
            LogHelper.WriteProgramLog(DateTime.Now.ToString() + "开始接收消息队列信息！");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                switch (mode)
                {
                    case "consume":
                        Run_Consume(brokerList, topics, cts.Token);
                        break;
                    case "manual":
                        Run_ManualAssign(brokerList, topics, cts.Token);
                        break;
                    default:
                        PrintUsage();
                        break;
                }
            }
            catch (Exception ex)
            {
                LogHelper.WriteProgramLog(DateTime.Now.ToString() + ex.Message);
            }

        }
        /// <summary>
        ///     In this example
        ///         - offsets are manually committed.
        ///         - no extra thread is created for the Poll (Consume) loop.
        /// </summary>
        public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = brokerList,
                GroupId = groupid,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            const int commitPeriod = 1;
            LogHelper.WriteProgramLog(DateTime.Now.ToString() + string.Format("brokerList为：", brokerList));
            using (var consumer = new Consumer<Ignore, string>(config))
            {
                consumer.Subscribe(topics);

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Console.WriteLine(string.Format("Topic: {0} Partition: {1} Offset: {2} {3}", consumeResult.Topic, consumeResult.Partition, consumeResult.Offset, consumeResult.Value));
                        LogHelper.WriteProgramLog(DateTime.Now.ToString() + string.Format(" Topic: {0} Partition: {1} Offset: {2} {3}", consumeResult.Topic, consumeResult.Partition, consumeResult.Offset, consumeResult.Value));
                        try
                        {
                            #region 这里开始执行业务代码逻辑
                            if (consumeResult.Topic == "")
                            {
                            }
                            #endregion
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(string.Format("同步信息发生错误: {0}", ex.Message));
                            LogHelper.WriteProgramLog(DateTime.Now.ToString() + string.Format("同步信息发生错误: {0}", ex.Message));
                        }

                        if (consumeResult.Offset % commitPeriod == 0)
                        {
                            // The Commit method sends a "commit offsets" request to the Kafka
                            // cluster and synchronously waits for the response. This is very
                            // slow compared to the rate at which the consumer is capable of
                            // consuming messages. A high performance application will typically
                            // commit offsets relatively infrequently and be designed handle
                            // duplicate messages in the event of failure.
                            var committedOffsets = consumer.Commit(consumeResult);
                            Console.WriteLine(string.Format("Committed offset: {0}", committedOffsets));
                            LogHelper.WriteProgramLog(DateTime.Now.ToString() + string.Format("Committed offset: {0}", committedOffsets));
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine(string.Format("Consume error: {0}", e.Error));
                        LogHelper.WriteProgramLog(DateTime.Now.ToString() + string.Format("Consume error: {0}", e.Error));
                    }
                }

                consumer.Close();
            }
        }
        /// <summary>
        ///     In this example
        ///         - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
        ///         - the consumer is manually assigned to a partition and always starts consumption
        ///           from a specific offset (0).
        /// </summary>
        public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                // the group.id property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerList,
                // partition offsets can be committed to a group even by consumers not
                // subscribed to the group. in this example, auto commit is disabled
                // to prevent this from occuring.
                EnableAutoCommit = true
            };

            using (var consumer = new Consumer<Ignore, string>(config))
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                consumer.OnError += (_, e)
                    => Console.WriteLine(string.Format("Error: {0}", e.Reason));

                consumer.OnPartitionEOF += (_, topicPartitionOffset)
                    => Console.WriteLine(string.Format("End of partition: {0}", topicPartitionOffset));

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);
                        Console.WriteLine(string.Format("Received message at {0}:${1}", consumeResult.TopicPartitionOffset, consumeResult.Message));
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine(string.Format("Consume error: {0}", e.Error));
                    }
                }

                consumer.Close();
            }
        }
        private static void PrintUsage()
        {
            Console.WriteLine("Usage: .. <poll|consume|manual> <broker,broker,..> <topic> [topic..]");
        }
    }
}
