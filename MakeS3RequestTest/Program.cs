using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MakeS3RequestTest
{
    class MakeS3RequestTest
    {
        private const string bucketName = "02003-gino-test";
        // 設定區域
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USWest2;
        private static IAmazonS3 clientS3;

        public static void Main()
        {
            var options = new CredentialProfileOptions
            {
                AccessKey = "AKIAYY2V6QTYTGMSKNO2",
                SecretKey = "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"
            };
            var profile = new Amazon.Runtime.CredentialManagement.CredentialProfile("gino355140", options);
            profile.Region = RegionEndpoint.APNortheast3;
            //var netSDKFile = new NetSDKCredentialsFile();
            //netSDKFile.RegisterProfile(profile);

            using (var client = new AmazonS3Client(options.AccessKey, options.SecretKey,profile.Region))
            {
                clientS3 = client;

                var response = client.ListBucketsAsync();
                var list = response.Result;

                //可列出所有Bucket，範例先以自己的做測試
                S3Bucket s3Bucket = list.Buckets.Find(x => x.BucketName == bucketName);

                Console.WriteLine("S3 Start");
                Console.WriteLine("---------------------");

                //"2021/01/SQL_Test.csv","123.csv"  測試用檔名
                string fileName = "2021/02/SQL_Test_big.csv";
                string sqlStr = "Select * from S3Object"; //除from S3Object固定外，其餘皆和一般SQL語法相同 

                //列出bucket and bucket內的東西
                DateTime start = new DateTime(2019, 09, 01); //起始時間
                DateTime end = new DateTime(2020, 01, 30);   //結束時間
                List<string> keys = getYearMotnhs(start, end);  //時間區間轉字串
                foreach (string key in keys)
                {
                    ListingObjectsAsync(key).Wait();
                }
                Console.WriteLine("---------------------");
                //讀取bucket and bucket內的東西
                var result = ReadObjectDataAsync(fileName).Result;
                Console.WriteLine("---------------------");
                //讀取bucket內的東西 by sql
                var result2 = GetSelectObjectContent("2021/01/SQL_Test.csv", sqlStr);
                Console.WriteLine("---------------------");

                //上傳物件
                //WritingAnObjectAsync().Wait();



                Console.WriteLine("---------------------");
                Console.WriteLine("S3 End");
            }

        }

        //列出bucket and bucket內的東西
        static async Task ListingObjectsAsync(string prefix)
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest
                {
                    BucketName = bucketName,
                    Prefix = prefix,
                    //Delimiter = "/",
                    MaxKeys = 99
                };
                do
                {
                    ListObjectsResponse response = await clientS3.ListObjectsAsync(request);
                    //Console.WriteLine(response.Name); //bucketName
                    // Process the response.
                    string key = string.Empty;
                    foreach (S3Object entry in response.S3Objects)
                    {
                        if (entry.Size != 0) //資料夾大小為0，排除
                        {
                            Console.WriteLine("key = {0} size = {1}",
                                entry.Key, entry.Size);
                            key = entry.Key;
                        }
                    }

                    // If the response is truncated, set the marker to get the next 
                    // set of keys.
                    if (response.IsTruncated)
                    {
                        request.Marker = response.NextMarker;
                    }
                    else
                    {
                        request = null;
                    }
                } while (request != null);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }

        //讀取bucket and bucket內的東西
        static async Task<string> ReadObjectDataAsync(string fileName)
        {
            string responseBody = "";
            try
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = fileName
                };
                using (GetObjectResponse response = await clientS3.GetObjectAsync(request))
                using (Stream responseStream = response.ResponseStream)
                using (StreamReader reader = new StreamReader(responseStream))
                {
                    string title = response.Metadata["x-amz-meta-title"]; // Assume you have "title" as medata added to the object.
                    string contentType = response.Headers["Content-Type"];
                    Console.WriteLine("Object metadata, Title: {0}", title);
                    Console.WriteLine("Content type: {0}", contentType);
                    responseBody = reader.ReadToEnd(); // Now you process the response body.
                    //csv to obj
                    var csv = responseBody;
                    var data = csv.FromCsv<List<TestClass>>();
                    var last = data.LastOrDefault();
                    Console.WriteLine("stream length: {0}", responseBody.Length);
                    Console.WriteLine("data count: {0}", data.Count);
                    return responseBody;
                }
            }
            catch (AmazonS3Exception e)
            {
                // If bucket or object does not exist
                Console.WriteLine("Error encountered ***. Message:'{0}' when reading object", e.Message);
                throw e;
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when reading object", e.Message);
                throw e;
            }
        }

        //讀取bucket內的東西 by sql
        static string GetSelectObjectContent(string fileName, string sqlStr)
        {
            string result = string.Empty;
            using (var s3Events = GetSelectObjectContentEventStream(fileName, sqlStr))
            {
                foreach (var ev in s3Events.Result)
                {
                    if (ev is RecordsEvent records)
                    {
                        using (var reader = new StreamReader(records.Payload))
                        {
                            result = reader.ReadToEnd();
                            int length = result.Length;
                            Console.WriteLine("stream length:{0}", length);
                            try
                            {
                                //csv to obj
                                var csv = result;
                                var data = csv.FromCsv<List<TestClass>>();
                                Console.WriteLine("data count:{0}", data.Count);
                            }
                            catch (Exception ex) { Console.WriteLine("error:{0}", ex.ToString()); }
                        }
                    }
                }
            }
            return result;
        }

        static async Task<ISelectObjectContentEventStream> GetSelectObjectContentEventStream(string fileName,string sqlStr)
        {
            //Select Object 設定
            SelectObjectContentRequest selectObject =
            new SelectObjectContentRequest()
            {
                Bucket = bucketName, //儲存貯體名稱
                Key = fileName, //讀取檔案名稱
                ExpressionType = ExpressionType.SQL, //Tyep為SQL
                Expression = sqlStr, //SQL語法
                InputSerialization = new InputSerialization() //讀取檔案的格式
                {
                    CSV = new CSVInput()
                    {
                        FileHeaderInfo = FileHeaderInfo.Use
                    }
                },
                OutputSerialization = new OutputSerialization() //輸出格式
                {
                    CSV = new CSVOutput()
                    {
                        QuoteFields = QuoteFields.Always,
                        FieldDelimiter = ","
                    }
                }
            };
            //取得Select Object 結果
            var response = await clientS3.SelectObjectContentAsync(selectObject);
            //回傳封包
            return response.Payload;
        }

        //上傳物件至bucket內
        static async Task WritingAnObjectAsync()
        {
            // You specify key names for these objects.
            string keyName1 = "Upload1.txt";
            string keyName2 = "2021/03/Upload2.csv";
            try
            {
                // 1. Put object-specify only key name for the new object.
                var putRequest1 = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName1,
                    ContentBody = "sample text 4444"
                };

                PutObjectResponse response1 = await clientS3.PutObjectAsync(putRequest1);

                if (response1.HttpStatusCode == System.Net.HttpStatusCode.OK)
                    Console.WriteLine("{0}上傳成功", putRequest1.Key);

                //clientS3.DeleteObjectAsync()

                List<TestClass> testClasses = new List<TestClass>();
                testClasses.Add(new TestClass() { Name = "a123", Company = "a456", Favorite_Color = "a789" });
                testClasses.Add(new TestClass() { Name = "b123", Company = "b456", Favorite_Color = "b789" });
                testClasses.Add(new TestClass() { Name = "c123", Company = "c456", Favorite_Color = "c789" });
                testClasses.Add(new TestClass() { Name = "d123", Company = "d456", Favorite_Color = "d789" });

                // 2. Put the object-set ContentType and add metadata.
                var putRequest2 = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName2,
                    //FilePath = filePath,
                    ContentBody = testClasses.ToCsv(),
                    ContentType = "text/csv"
                };

                putRequest2.Metadata.Add("x-amz-meta-title", "someTitle");
                PutObjectResponse response2 = await clientS3.PutObjectAsync(putRequest2);

                if (response2.HttpStatusCode == System.Net.HttpStatusCode.OK)
                    Console.WriteLine("{0}上傳成功", putRequest2.Key);

            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine(
                        "Error encountered ***. Message:'{0}' when writing an object"
                        , e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine(
                    "Unknown encountered on server. Message:'{0}' when writing an object"
                    , e.Message);
            }
        }

        //分段上傳範例
        private static async Task UploadFileAsync()
        {
            try
            {
                string filePath = string.Empty;
                string keyName = string.Empty;

                var fileTransferUtility =
                    new TransferUtility(clientS3);

                // Option 1. Upload a file. The file name is used as the object key name.
                await fileTransferUtility.UploadAsync(filePath, bucketName);
                Console.WriteLine("Upload 1 completed");

                // Option 2. Specify object key name explicitly.
                await fileTransferUtility.UploadAsync(filePath, bucketName, keyName);
                Console.WriteLine("Upload 2 completed");

                // Option 3. Upload data from a type of System.IO.Stream.
                using (var fileToUpload =
                    new FileStream(filePath, FileMode.Open, FileAccess.Read))
                {
                    await fileTransferUtility.UploadAsync(fileToUpload,
                                               bucketName, keyName);
                }
                Console.WriteLine("Upload 3 completed");

                // Option 4. Specify advanced settings.
                var fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName = bucketName,
                    FilePath = filePath,
                    StorageClass = S3StorageClass.StandardInfrequentAccess,
                    PartSize = 6291456, // 6 MB.
                    Key = keyName,
                    CannedACL = S3CannedACL.PublicRead
                };
                fileTransferUtilityRequest.Metadata.Add("param1", "Value1");
                fileTransferUtilityRequest.Metadata.Add("param2", "Value2");

                await fileTransferUtility.UploadAsync(fileTransferUtilityRequest);
                Console.WriteLine("Upload 4 completed");
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }

        }

        public class TestClass
        {
            public string Name { get; set; }

            public string Company { get; set; }

            public string Favorite_Color { get; set; }

            public string Test { get { return this.Name + this.Company; } }
        }

        public static List<string> getYearMotnhs(DateTime start, DateTime end)
        {
            List<string> result = new List<string>();
            int overYearNum = end.Year - start.Year; //是否跨年度
            int overMonthNum = end.Month - start.Month; //差的月數

            if (overYearNum == 0)
            {
                if (overMonthNum == 0) //當月份
                    result.Add(start.ToString("yyyy") + "/" + start.ToString("MM"));
                else
                {
                    for (int i = 0; i <= overMonthNum; i++)
                        result.Add(start.ToString("yyyy") + "/" + start.AddMonths(i).ToString("MM"));
                }
            }
            else
            {
                switch (overYearNum)
                {
                    case 1:
                        for (int i = 0; i <= (12 - start.Month); i++) //初始 ~ 當年12月
                            result.Add(start.ToString("yyyy") + "/" + start.AddMonths(i).ToString("MM"));

                        for (int i = 0; i < end.Month; i++) //隔年1月 ~ 結束(倒退算)
                            result.Add(end.ToString("yyyy") + "/" + end.AddMonths(-i).ToString("MM"));
                        break;
                    default:
                        for (int i = 0; i <= (12 - start.Month); i++) //初始 ~ 當年12月
                            result.Add(start.ToString("yyyy") + "/" + start.AddMonths(i).ToString("MM"));
        
                        for (int i = 1; i < overYearNum; i++) //跨年度
                        {
                            DateTime month = new DateTime(start.AddYears(i).Year, 1, 1);
                            for (int j = 0; j < 12; j++) // 1~12月
                            {
                                result.Add(start.AddYears(i).ToString("yyyy") + "/" + month.AddMonths(j).ToString("MM"));
                            }
                        }
                            
                        for (int i = 0; i < end.Month; i++) //隔年1月 ~ 結束(倒退算)
                            result.Add(end.ToString("yyyy") + "/" + end.AddMonths(-i).ToString("MM"));

                        break;
                }
            }

            return result;
        }
    }
}
