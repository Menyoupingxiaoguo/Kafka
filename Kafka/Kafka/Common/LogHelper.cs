/*----------------------------------------------------------------
* 项目名称 ：Kafka.Common
* 项目描述 ：
* 类 名 称 ：LogHelper
* 类 描 述 ：
* 所在的域 ：YANGKANG-PC
* 命名空间 ：Kafka.Common
* 机器名称 ：YANGKANG-PC 
* 作    者 ：Administrator
* 创建时间 ：2019/8/6 14:02:22
* 更新时间 ：2019/8/6 14:02:22
* 版 本 号 ：v1.0.0.0
*******************************************************************
* Copyright @ Administrator 2019. All rights reserved.
*******************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Common
{
    public class LogHelper
    {
        static object locker = new object();
        /// <summary>  
        /// 写入所有日志  
        /// </summary>  
        /// <param name="logs">日志列表，每条日志占一行</param>  
        public static void WriteProgramLog(params string[] logs)
        {
            lock (locker)
            {
                string path = typeof(Program).Assembly.Location; //第一句代码 是获取Program 这个类所在 程序集dll 的物理路径  
                string pro = Path.GetDirectoryName(path); //第二句代码 是获取这个dll程序集所在的目录位置  

                string LogAddress = pro + @"\log";
                if (!Directory.Exists(LogAddress + "\\ProgramLog"))
                {
                    Directory.CreateDirectory(LogAddress + "\\ProgramLog");
                }
                LogAddress = string.Concat(LogAddress, "\\ProgramLog\\",
                DateTime.Now.Year, '-', DateTime.Now.Month, '-',
                DateTime.Now.Day, "_Log.log");
                StreamWriter sw = new StreamWriter(LogAddress, true);
                foreach (string log in logs)
                {
                    sw.WriteLine(log + "  at  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                }
                sw.Close();
            }
        }
        /// <summary>  
        /// 写入所有日志到指定文件夹
        /// </summary>  
        /// <param name="log">日志</param>  
        public static void WriteProgramLogInFolder(string log, string Folder)
        {
            lock (locker)
            {
                string pro = System.AppDomain.CurrentDomain.BaseDirectory;
                string LogAddress = pro + @"\log";
                if (!Directory.Exists(LogAddress + "\\" + Folder))
                {
                    Directory.CreateDirectory(LogAddress + "\\" + Folder);
                }
                LogAddress = string.Concat(LogAddress, "\\" + Folder + "\\",
                DateTime.Now.Year, '-', DateTime.Now.Month, '-',
                DateTime.Now.Day, "_Log.log");
                StreamWriter sw = new StreamWriter(LogAddress, true);
                sw.WriteLine(log + "  at  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                sw.Close();
            }
        }
    }
}
