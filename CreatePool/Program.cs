/*
 *Read 30 files( 10 of 1 MB ,10 of 2 MB and 10 of 4 MB) in parallel with 5 buffers of 1MB,
 * 5 buffers of 2 MB and 5 buffers of 4 MB using concurrent append in one file. 		
 */
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

        private byte[] buffer1;
        private byte[] buffer2;
        private byte[] buffer3;

        //private ArrayPool<byte> samePool = ArrayPool<byte>.Shared;
        private ArrayPool<byte> Pool1 = ArrayPool<byte>.Create(One_MB,5);
        private ArrayPool<byte> Pool2 = ArrayPool<byte>.Create(Two_MB,5);
        private ArrayPool<byte> Pool3 = ArrayPool<byte>.Create(Four_MB,5);

        public void SendData(AdlsClient c,string filename, string path)
        {
            FileInfo f = new FileInfo(path); 
            long length = f.Length;

            while (length>0)
            {
                // sending data via 1 MB buffer if data is less than/equal to 1 MB
                if (length - One_MB <= 0)
                {
                   
                    buffer1 = Pool1.Rent(One_MB); 
         
                    length = length - buffer1.Length;
                    lock (buffer1)
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer1, 0, buffer1.Length); 
                            c.ConcurrentAppend(filename, true, buffer1, 0, (int) buffer1.Length);
                            Array.Clear(buffer1, 0, (int) buffer1.Length);
                        }
                    }

                    Pool1.Return(buffer1);
                }
                //sending data via 2 MB buffer if data is more than 1 MB but less than 2 MB
                else if(length - Two_MB <= 0)
                {
                 
                    buffer2 = Pool2.Rent(Two_MB); 
                 
                    length = length - buffer2.Length;
                    lock (buffer2)
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer2, 0, buffer2.Length); 
                            c.ConcurrentAppend(filename, true, buffer2 , 0, (int) buffer2.Length); 
                            Array.Clear(buffer2, 0, (int) buffer2.Length);
                        }
                    }

                    Pool2.Return(buffer2);
                }
                //sending data via 4 MB if data is more than 2 MB
                else
                {
                    
                    buffer3= Pool3.Rent(Four_MB); 

                    length = length - buffer3.Length;
                    lock (buffer3)
                    {
                        using (var file = new FileStream(path, FileMode.Open))
                        { 
                            file.Read(buffer3, 0, buffer3.Length); 
                            c.ConcurrentAppend(filename, true, buffer3 , 0, (int) buffer3.Length); 
                            Array.Clear(buffer3, 0, (int) buffer3.Length);
                        }
                    }

                    Pool3.Return(buffer3);
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
                //file that wil be created and appended
                string filename = @"YOLO_Chahat20.txt";
                string[] path = new string[30];

                Parallel.For(0, 30, i => {

                    if (i < 10)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\ADLS\Report_10\Input\1MB\" + (i + 1) + ".txt";
                    }
                    else if(i < 20)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\ADLS\Report_10\Input\2MB\" + ((i%10) + 1) + ".txt";
                    }
                    else
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\ADLS\Report_10\Input\4MB\" + ((i%10) + 1) + ".txt";
                    }

                    obj.SendData(client,filename, path[i]);

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