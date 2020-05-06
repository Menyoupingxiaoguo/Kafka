/*----------------------------------------------------------------
* 项目名称 ：Kafka.Common
* 项目描述 ：
* 类 名 称 ：KafkaProducer
* 类 描 述 ：
* 所在的域 ：YANGKANG-PC
* 命名空间 ：Kafka.Common
* 机器名称 ：YANGKANG-PC 
* 作    者 ：Administrator
* 创建时间 ：2019/8/6 14:03:09
* 更新时间 ：2019/8/6 14:03:09
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
using System.Threading.Tasks;

namespace Kafka.Common
{
    /// <summary>
    /// Kafka消息生产者
    /// </summary>
    public class KafkaProducer
    {
        public static string brokerUrl = ConfigurationManager.AppSettings["Broker"];
        public static string topic = ConfigurationManager.AppSettings["ProduceTopic"];
        public static string groupid = ConfigurationManager.AppSettings["GroupID"];
        private static readonly object Locker = new object();
        private static Producer<string, string> _producer;
        /// <summary>
        /// 单例生产
        /// </summary>
        public KafkaProducer()
        {
            if (_producer == null)
            {
                lock (Locker)
                {
                    if (_producer == null)
                    {
                        var config = new ProducerConfig
                        {
                            BootstrapServers = brokerUrl
                        };
                        _producer = new Producer<string, string>(config);
                    }
                }
            }
        }

        /// <summary>
        /// 生产消息并发送消息
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="message">需要传送的消息</param>
        public static void Produce(string key, string message, string topic)
        {
            bool result = false;
            new KafkaProducer();
            if (string.IsNullOrEmpty(message) || string.IsNullOrWhiteSpace(message) || message.Length <= 0)
            {
                throw new ArgumentNullException("消息内容不能为空！");
            }
            var deliveryReport = _producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = message });
            deliveryReport.ContinueWith(task =>
            {
                Console.WriteLine("Producer：" + _producer.Name + "\r\nTopic：" + topic + "\r\nPartition：" + task.Result.Partition + "\r\nOffset：" + task.Result.Offset + "\r\nMessage：" + task.Result.Value + "\r\nResult：" + result);
                LogHelper.WriteProgramLogInFolder(DateTime.Now.ToString() + "Producer：" + _producer.Name + "\r\nTopic：" + topic + "\r\nPartition：" + task.Result.Partition + "\r\nOffset：" + task.Result.Offset + "\r\nMessage：" + task.Result.Value, "KinderAssignLog");
            });
            _producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
