using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;

using Newtonsoft.Json;

namespace Microsoft.CosmosDB.PITRWithRestore.Logger
{
    public class LogAnalyticsLogger : ILogger
    {
        /// <summary>
        /// Workspace Id for Log Analytics
        /// </summary>
        private string WorkspaceId;

        /// <summary>
        /// Application Key to authenticate with Azure Log Analytics
        /// </summary>
        private string ApplicationKey;

        /// <summary>
        /// API version to use when writing to Log Analytics
        /// </summary>
        private string ApiVersion;

        /// <summary>
        /// Log type of the logs being pushed to Azure Log Analytics
        /// </summary>
        private string LogType;

        /// <summary>
        /// Constructor for an instance of LogAnalyticsLogger to interact with Azure Log Analytics
        /// </summary>
        public LogAnalyticsLogger()
        {
            this.WorkspaceId = ConfigurationManager.AppSettings["LogAnalyticsWorkSpaceId"];
            this.ApplicationKey = ConfigurationManager.AppSettings["LogAnalyticsApplicationKey"];
            this.ApiVersion = ConfigurationManager.AppSettings["ApiVersion"];
            this.LogType = ConfigurationManager.AppSettings["LogType"];
        }

        /// <summary>
        /// Implementation to push input list of messages to Azure Log Analytics
        /// </summary>
        /// <param name="messageToLog">Message to be pushed to Log Analytics</param>
        public void WriteMessage(string messageToLog)
        {
            string jsonToLog = JsonConvert.SerializeObject(
                new
                {
                    id = Guid.NewGuid().ToString(),
                    datetime = DateTime.Now,
                    message = messageToLog
                });

            string requestUriString = $"https://{this.WorkspaceId}.ods.opinsights.azure.com/api/logs?api-version={this.ApiVersion}";
            DateTime dateTime = DateTime.UtcNow;
            string dateString = dateTime.ToString("r");

            string signature = GetSignature("POST", jsonToLog.Length, "application/json", dateString, "/api/logs");
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUriString);
            request.ContentType = "application/json";
            request.Method = "POST";
            request.Headers["Log-Type"] = LogType;
            request.Headers["x-ms-date"] = dateString;
            request.Headers["Authorization"] = signature;
            byte[] content = Encoding.UTF8.GetBytes(jsonToLog);
            using (Stream requestStreamAsync = request.GetRequestStream())
            {
                requestStreamAsync.Write(content, 0, content.Length);
            }
            using (HttpWebResponse responseAsync = (HttpWebResponse)request.GetResponse())
            {
                //if (responseAsync.StatusCode != HttpStatusCode.OK && responseAsync.StatusCode != HttpStatusCode.Accepted)
                //{
                //    Stream responseStream = responseAsync.GetResponseStream();
                //    if (responseStream != null)
                //    {
                //        using (StreamReader streamReader = new StreamReader(responseStream))
                //        {
                //            throw new Exception(streamReader.ReadToEnd());
                //        }
                //    }
                //}
            }
        }

        private string GetSignature(string method, int contentLength, string contentType, string date, string resource)
        {
            string message = $"{method}\n{contentLength}\n{contentType}\nx-ms-date:{date}\n{resource}";
            byte[] bytes = Encoding.UTF8.GetBytes(message);
            using (HMACSHA256 encryptor = new HMACSHA256(Convert.FromBase64String(this.ApplicationKey)))
            {
                return $"SharedKey {WorkspaceId}:{Convert.ToBase64String(encryptor.ComputeHash(bytes))}";
            }
        }
    }
}
