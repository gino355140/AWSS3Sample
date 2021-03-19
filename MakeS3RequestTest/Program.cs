using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
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
        // Specify your bucket region (an example region is shown).
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

                S3Bucket s3Bucket = list.Buckets.Find(x => x.BucketName == bucketName);

                Console.WriteLine("start");
                //列出bucket and bucket內的東西
                //ListingObjectsAsync().Wait();

                //讀取bucket and bucket內的東西
                //var result = ReadObjectDataAsync("SQL_Test.txt").Result;
                //List<TestClass> objs = JsonConvert.DeserializeObject<List<TestClass>>(result);
                //objs = objs.Where(x => x.Company == "AMAZON").ToList();

                using (var s3Events = GetSelectObjectContentEventStream())
                {
                    foreach (var ev in s3Events.Result)
                    {
                        if (ev is RecordsEvent records)
                        {
                            using (var reader = new StreamReader(records.Payload, System.Text.Encoding.UTF8))
                            {
                                string result = reader.ReadToEnd();
                                int length = result.Length;
                                Console.WriteLine(result);
                            }
                        }
                    }
                }
            }

        }

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
                    Console.WriteLine("Body: {0}", responseBody);

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

        static async Task ListingObjectsAsync()
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest
                {
                    BucketName = bucketName,
                    MaxKeys = 10
                };
                do
                {

                    Console.WriteLine("Request Start");
                    ListObjectsResponse response = await clientS3.ListObjectsAsync(request);
                    Console.WriteLine(response.Name);
                    // Process the response.
                    string key = string.Empty;
                    foreach (S3Object entry in response.S3Objects)
                    {
                        Console.WriteLine("key = {0} size = {1}",
                            entry.Key, entry.Size);
                        key = entry.Key;
                    }


                    // Create a GetObject request
                    GetObjectRequest objRequest = new GetObjectRequest
                    {
                        BucketName = bucketName,
                        Key = key
                    };

                    using (GetObjectResponse objResponse = await clientS3.GetObjectAsync(objRequest))
                    {
                        using (StreamReader reader = new StreamReader(objResponse.ResponseStream))
                        {
                            string contents = reader.ReadToEnd();
                            Console.WriteLine("Object - " + objResponse.Key);
                            Console.WriteLine(" Version Id - " + objResponse.VersionId);
                            Console.WriteLine(" Contents - " + contents);
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

        static async Task<ISelectObjectContentEventStream> GetSelectObjectContentEventStream()
        {
            SelectObjectContentRequest selectObject =
            new SelectObjectContentRequest()
            {
                Bucket = bucketName,
                Key = "2021/02/SQL_Test_big.csv",

                ExpressionType = ExpressionType.SQL,
                //Expression = "select name,company from S3Object s where s.company = 'AMAZON'",
                Expression = "Select * From S3Object",
                InputSerialization = new InputSerialization()
                {
                    CSV = new CSVInput()
                    {
                        FileHeaderInfo = FileHeaderInfo.Use
                    }
                },
                OutputSerialization = new OutputSerialization()
                {
                    CSV = new CSVOutput()
                    {
                        QuoteFields = QuoteFields.Always,
                        FieldDelimiter = ";"
                    }
                }
            };
            var response = await clientS3.SelectObjectContentAsync(selectObject);

            var a = response.ContentLength;

            return response.Payload;
        }

        class TestClass
        {
            public string Name { get; set; }

            public string Company { get; set; }

            public string Favorite_Color { get; set; }
        }
    }
}
