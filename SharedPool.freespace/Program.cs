//optimised report to use the buffer which is empty 

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

        private int[] arr1 =  { -1,-1,-1,-1,-1};
        private int[] arr2 =  { -1,-1,-1,-1,-1};
        private int[] arr3 =  { -1,-1,-1,-1,-1};

        private ArrayPool<byte> samePool = ArrayPool<byte>.Shared;

        BlockingCollection<int> b1 = new BlockingCollection<Int32>(1);
        BlockingCollection<int> b2 = new BlockingCollection<Int32>(1);
        BlockingCollection<int> b3 = new BlockingCollection<Int32>(1);

        public void SendData(AdlsClient c, int i,string filename, string path)
        {
            i = i % 5; 
            FileInfo f = new FileInfo(path); 
            long length = f.Length;
            while (length>0)
            { 
                // sending data via 1 MB buffer if data is less than/equal to 1 MB
                if (length - One_MB <= 0)
                {
                    if (buffer1[i] == null)
                    {
                        b1.Add(1);
                        if (buffer1[i] == null)
                        { 
                            buffer1[i] = samePool.Rent(One_MB); 
                            arr1[i] = 0;
                        }
                        b1.Take(); 
                    }
                    length = length - buffer1[i].Length;
                    for(int j=0;j<arr1.Length;j++)
                    {
                        if(arr1[j]==0)
                        {
                            i = j;
                            j= arr1.Length;
                        }
                    }
                    lock (buffer1[i])
                    {
                        arr1[i] = 1;
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer1[i], 0, buffer1[i].Length); 
                            c.ConcurrentAppend(filename, true, buffer1[i] , 0, (int) buffer1[i].Length); 
                            Array.Clear(buffer1[i], 0, (int) buffer1[i].Length);
                        }
                        arr1[i] = 0;
                    }
                    samePool.Return(buffer1[i]);
                }
                //sending data via 2 MB buffer if data is more than 1 MB but less than/ equal to 2 MB
                else if(length - Two_MB <= 0)
                {
                    if (buffer2[i] == null)
                    {
                        b2.Add(1);
                        if (buffer2[i] == null)
                        { 
                            buffer2[i] = samePool.Rent(Two_MB); 
                            arr2[i] = 0;
                        }
                        b2.Take(); 
                    }
                    length = length - buffer2[i].Length;
                    for(int j=0;j<arr2.Length;j++)
                    {
                        if(arr2[j]==0)
                        {
                            i = j;
                            j= arr2.Length;
                        }
                    }
                    lock (buffer2[i])
                    {
                        arr2[i] = 1;
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer2[i], 0, buffer2[i].Length); 
                            c.ConcurrentAppend(filename, true, buffer2[i] , 0, (int) buffer2[i].Length); 
                            Array.Clear(buffer2[i], 0, (int) buffer2[i].Length);
                        }
                        arr2[i]=0;
                    }

                    samePool.Return(buffer2[i]);
                }
                //sending data via 4 MB if data is more than 2 MB
                else
                {
                    if (buffer3[i] == null)
                    {
                        b3.Add(1);
                        if (buffer3[i] == null)
                        { 
                            buffer3[i] = samePool.Rent(Four_MB); 
                            arr3[i] = 0;
                        }
                        b3.Take(); 
                    }
                    length = length - buffer3[i].Length;
                    for(int j=0;j<arr3.Length;j++)
                    {
                        if(arr3[j]==0)
                        {
                            i = j;
                            j= arr3.Length;
                        }
                    }
                    lock (buffer3[i])
                    {
                        arr3[i] =1;
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer3[i], 0, buffer3[i].Length); 
                            c.ConcurrentAppend(filename, true, buffer3[i] , 0, (int) buffer3[i].Length); 
                            
                            Array.Clear(buffer3[i], 0, (int) buffer3[i].Length);
                        }
                        arr3[i] =0;
                    }

                    samePool.Return(buffer3[i]);
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
                string filename = @"A2-COE.txt";
                string[] path = new string[30];

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

                    obj.SendData(client, i, filename, path[i]);

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
