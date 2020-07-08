//optimised report 10  by inclusing randomized bufferring
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Buffers;
using System.Collections.Concurrent;

using Microsoft.Azure.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;

namespace Report_7
{   
    public class Buffering
    {
        private const int One_MB = 1050000;
        private const int Two_MB = 2100000;
        private const int Four_MB = 4200000;

        private byte[][] buffer1 = new byte[5][];
        private byte[][] buffer2 = new byte[5][];
        private byte[][] buffer3 = new byte[5][];

        private ArrayPool<byte> samePool = ArrayPool<byte>.Shared;

        BlockingCollection<int> b1 = new BlockingCollection<Int32>(1);
        BlockingCollection<int> b2 = new BlockingCollection<Int32>(1);
        BlockingCollection<int> b3 = new BlockingCollection<Int32>(1);

        public void SendData(AdlsClient c, int random,string filename, string path)
        {
            FileInfo f = new FileInfo(path); 
            long length = f.Length;

            while (length>0)
            {
                // sending data via 1 MB buffer if data is less than/equal to 1 MB
                if (length - One_MB <= 0)
                {
                    if (buffer1[random] == null)
                    {
                        b1.Add(1);
                        if (buffer1[random] == null)
                        { 
                            buffer1[random] = samePool.Rent(One_MB); 
                        }
                        b1.Take(); 
                    }
                    length = length - buffer1[random].Length;
                    lock (buffer1[random])
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer1[random], 0, buffer1[random].Length); 
                            c.ConcurrentAppend(filename, true, buffer1[random] , 0, (int) buffer1[random].Length); 
                            Array.Clear(buffer1[random], 0, (int) buffer1[random].Length);
                        }
                    }
                    samePool.Return(buffer1[random]);
                }
                //sending data via 2 MB buffer if data is more than 1 MB but less than/ equal to 2 MB
                else if(length - Two_MB <= 0)
                {
                    if (buffer2[random] == null)
                    {
                        b2.Add(1);
                        if (buffer2[random] == null)
                        { 
                            buffer2[random] = samePool.Rent(Two_MB); 
                        }
                        b2.Take(); 
                    }
                    length = length - buffer2[random].Length;
                    lock (buffer2[random])
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer2[random], 0, buffer2[random].Length); 
                            c.ConcurrentAppend(filename, true, buffer2[random] , 0, (int) buffer2[random].Length); 
                            Array.Clear(buffer2[random], 0, (int) buffer2[random].Length);
                        }
                    }

                    samePool.Return(buffer2[random]);
                }
                //sending data via 4 MB if data is more than 2 MB
                else
                {
                    if (buffer3[random] == null)
                    {
                        b3.Add(1);
                        if (buffer3[random] == null)
                        { 
                            buffer3[random] = samePool.Rent(Four_MB); 
                        }
                        b3.Take(); 
                    }
                    length = length - buffer3[random].Length;
                    lock (buffer3[random])
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 

                            file.Read(buffer3[random], 0, buffer3[random].Length); 
                            c.ConcurrentAppend(filename, true, buffer3[random] , 0, (int) buffer3[random].Length); 
                            Array.Clear(buffer3[random], 0, (int) buffer3[random].Length);
                        }
                    }

                    samePool.Return(buffer3[random]);
                }
            }
               
        }

    }
    public class Program
    {

        private static string applicationId = "35fe4d8f-b30e-40ed-8cdf-fee0216569de";     // Also called client id
        private static string clientSecret = ".P0KipHqZAj18k-.t8_-WN4v~90.Jst08h";
        private static string tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        private static string adlsAccountFQDN = "marstest.azuredatalakestore.net";   // full account FQDN, not just the account name like example.azure.datalakestore.net
                                                                                  
        public static void Main(string[] args)
        {
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);
            
            var obj = new Buffering();

              try
            {
                 string filename = @"2systems.txt";
                 string[] path = new string[30];
 
                Random random = new Random(); 
                object syncLock = new object(); 
                int last = -1;
                int RandomNumber(int min, int max)
                {
                    lock(syncLock) { 
                    int num;
                    do {
                       num = random.Next(5);
                    } while(num == last);
                    last = num;
                       
                    return num;
                  }
                }
                //------------------------//
                Parallel.For(0,30, i => {

                    if (i < 10)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\1MB\" + (i + 1) + ".txt";
                    }
                    else if (i < 20)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\2MB\" + ((i % 10) + 1) + ".txt";
                    }
                    else
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\4MB\" + ((i % 10) + 1) + ".txt";
                    }

                    obj.SendData(client, RandomNumber(0,5), filename, path[i]);

                });
            }
            catch (AdlsException e)
            {
                Console.WriteLine(e);
            }

            Console.WriteLine("Done. Press ENTER to continue ...");
            Console.ReadLine();

        }
    }
}
