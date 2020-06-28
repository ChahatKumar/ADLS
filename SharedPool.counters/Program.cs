//optimised report 10 to prevent blocking calls

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

        public void SendData(AdlsClient c, int counter1,int counter2,int counter3,string filename, string path)
        {
           // i = i % 5; 
            FileInfo f = new FileInfo(path); 
            long length = f.Length;

            while (length>0)
            {
                // sending data via 1 MB buffer if data is less than/equal to 1 MB
                if (length - One_MB <= 0)
                {  
                    if (buffer1[counter1] == null)
                    {
                        b1.Add(1);
                        if (buffer1[counter1] == null)
                        { 
                            buffer1[counter1] = samePool.Rent(One_MB); 
                        }
                        b1.Take(); 
                    }

                    length = length - buffer1[counter1].Length;

                    lock (buffer1[counter1])
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                              Console.WriteLine("buffer 1 -  {0}",counter1);
                            file.Read(buffer1[counter1], 0, buffer1[counter1].Length); 
                            c.ConcurrentAppend(filename, true, buffer1[counter1] , 0, (int) buffer1[counter1].Length); 
                            Array.Clear(buffer1[counter1], 0, (int) buffer1[counter1].Length);
                        }
                    }
                    samePool.Return(buffer1[counter1]);
                }
                //sending data via 2 MB buffer if data is more than 1 MB but less than/ equal to 2 MB
                else if(length - Two_MB <= 0)
                {
                    if (buffer2[counter2] == null)
                    {
                        b2.Add(1);
                        if (buffer2[counter2] == null)
                        { 
                            buffer2[counter2] = samePool.Rent(Two_MB); 
                        }
                        b2.Take(); 
                    }
                    length = length - buffer2[counter2].Length;
                    lock (buffer2[counter2])
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            Console.WriteLine("buffer 2 -  {0}",counter2);
                            file.Read(buffer2[counter2], 0, buffer2[counter2].Length); 
                            c.ConcurrentAppend(filename, true, buffer2[counter2] , 0, (int) buffer2[counter2].Length); 
                            Array.Clear(buffer2[counter2], 0, (int) buffer2[counter2].Length);
                        }
                    }

                    samePool.Return(buffer2[counter2]);
                }
                //sending data via 4 MB if data is more than 2 MB
                else
                {
                    if (buffer3[counter3] == null)
                    {
                        b3.Add(1);
                        if (buffer3[counter3] == null)
                        { 
                            buffer3[counter3] = samePool.Rent(Four_MB); 
                        }
                        b3.Take(); 
                    }
                    length = length - buffer3[counter3].Length;
                    lock (buffer3[counter3])
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            Console.WriteLine("buffer 3  - {0}",counter3);
                            file.Read(buffer3[counter3], 0, buffer3[counter3].Length); 
                            c.ConcurrentAppend(filename, true, buffer3[counter3] , 0, (int) buffer3[counter3].Length); 
                            Array.Clear(buffer3[counter3], 0, (int) buffer3[counter3].Length);
                        }
                    }
                    samePool.Return(buffer3[counter3]);
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
                                                                              
                        static int counter1 =0;
static int counter2 =0;
        static int counter3 =0;
        public static void Main(string[] args)
        {
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);

            BlockingCollection<int> b = BlockingCollection<int>(1);
            
            var obj = new Buffering();

            try
            {

                string filename = @"A2-COE.txt";
                string[] path = new string[30];

                Parallel.For(0 ,30, i => {
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
                    b.Add(1);
                    {
                        if(counter1>4)
                        {
                            counter1 =0;
                        }
                        if(counter2 > 4)
                        {
                            counter2 =0;
                        }
                        if(counter3 > 4)
                        {
                            counter3=0;
                        }
                        obj.SendData(client,counter1,counter2,counter3,filename,path[i]);
                        counter1++;
                        counter2++;
                        counter3++;
                    }
                    b.Take();

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
