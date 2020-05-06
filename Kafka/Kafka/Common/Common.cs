/*----------------------------------------------------------------
* 项目名称 ：Kafka.Common
* 项目描述 ：
* 类 名 称 ：Common
* 类 描 述 ：
* 所在的域 ：YANGKANG-PC
* 命名空间 ：Kafka.Common
* 机器名称 ：YANGKANG-PC 
* 作    者 ：Administrator
* 创建时间 ：2019/8/6 14:01:51
* 更新时间 ：2019/8/6 14:01:51
* 版 本 号 ：v1.0.0.0
*******************************************************************
* Copyright @ Administrator 2019. All rights reserved.
*******************************************************************/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Common
{
    public static class Common
    {
        /// <summary>
        /// ToInt32 扩展方法 
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="defV"></param>
        /// <returns></returns>
        public static int ToInt32(this object obj, int defV = 0)
        {
            int temp = 0;
            try
            {
                temp = Convert.ToInt32(obj);
            }
            catch
            {
                temp = defV;
            }
            return temp;
        }
        /// <summary>
        /// 获取当前时间时间戳
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static long GetTimeStamp(this DateTime time)
        {
            TimeSpan ts = time.ToUniversalTime() - new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return Convert.ToInt64(ts.TotalMilliseconds);
        }
        /// <summary>
        /// 时间戳转为C#格式时间
        /// </summary>
        /// <param name="timeStamp">Unix时间戳格式</param>
        /// <returns>C#格式时间</returns>
        public static DateTime StampToDateTime(string timeStamp)
        {
            DateTime dtStart = TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
            string strZero = "0000000";
            if (timeStamp.Length == 13)
                strZero = "0000";
            long lTime = long.Parse(timeStamp + strZero);
            TimeSpan toNow = new TimeSpan(lTime);
            return dtStart.Add(toNow);
        }
    }
}
