//optimised report 10  to use buffers through blocking collection 

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

        BlockingCollection<byte []> b1 = new BlockingCollection<byte []>(5);
        BlockingCollection<byte []> b2 = new BlockingCollection<byte []>(5);
        BlockingCollection<byte []> b4 = new BlockingCollection<byte []>(5);

        BlockingCollection<int> sync1 = new BlockingCollection<int>(1);
        BlockingCollection<int> sync2 = new BlockingCollection<int>(1);
        BlockingCollection<int> sync4 = new BlockingCollection<int>(1);

        public void SendData(AdlsClient c,string filename, string path)
        {
            FileInfo f = new FileInfo(path); 
            long length = f.Length;

            while (length>0)
            {
                // sending data via 1 MB buffer if data is less than/equal to 1 MB
                if (length - One_MB <= 0)
                {
                    sync1.Add(1);

                    if(b1.Count <= 4)
                       {
                         byte [] temp = new byte[One_MB];
                         b1.Add(temp);
                         Console.WriteLine("buffer count after- {0}",b1.Count);
                       }                

                    sync1.Take();

                    sync1.Add(1);
                   
                     byte[] buffer1 = b1.Take();
                     b1.Add(buffer1);
                    
                    sync1.Take();
                  
                   length = length - buffer1.Length;
                   lock (buffer1)
                      {
                          using (var file = new FileStream(path, FileMode.Open))
                          { 
                            file.Read(buffer1,0, buffer1.Length); 
                            c.ConcurrentAppend(filename, true, buffer1, 0, buffer1.Length); 
                            Array.Clear(buffer1, 0, buffer1.Length);
                          }
                      } 
                    
                }

                //sending data via 2 MB buffer if data is more than 1 MB but less than/ equal to 2 MB
                else if(length - Two_MB <= 0)
                {
                  
                       sync2.Add(1);
                       if(b2.Count <= 4)
                       {
                         byte [] temp = new byte[Two_MB];
                         b2.Add(temp);
                       }

                     sync2.Take();
                   
                     sync2.Add(1);
                   
                     byte[] buffer2 = b2.Take();
                     b2.Add(buffer2);
                    
                    sync2.Take();
                  
                   length = length - buffer2.Length;
                   lock (buffer2)
                      {
                          using (var file = new FileStream(path, FileMode.Open))
                          { 
                            file.Read(buffer2,0, buffer2.Length); 
                            c.ConcurrentAppend(filename, true, buffer2, 0, buffer2.Length); 
                            Array.Clear(buffer2, 0, buffer2.Length);
                          }
                      } 
                   
                }
                //sending data via 4 MB if data is more than 2 MB
                else
                {
                  
                       sync4.Add(1);
                       if(b4.Count <= 4)
                       {
                         byte [] temp = new byte[Four_MB];
                         b4.Add(temp);
                       }

                       sync4.Take();
                   
                     sync4.Add(1);
                   
                     byte[] buffer4 = b4.Take();
                     b4.Add(buffer4);
                    
                    sync4.Take();
                  

                   length = length - buffer4.Length;
                   lock (buffer4)
                      {
                          using (var file = new FileStream(path, FileMode.Open))
                          { 
                            file.Read(buffer4 ,0, buffer4.Length); 
                            c.ConcurrentAppend(filename, true, buffer4, 0, buffer4.Length); 
                            Array.Clear(buffer4, 0, buffer4.Length);
                          }
                      } 
                }
            }
         }
    }
    public class Program
    {

        private static string applicationId = "35fe4d8f-b30e-40ed-8cdf-fee0216569de";     // Also called client id
        private static string clientSecret = ".P0KipHqZAj18k-.t8_-WN4v~90.Jst08h";
        private static string tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        private static string adlsAccountFQDN = "marstest.azuredatalakestore.net";   // full account FQDN
                                                                                  
        public static void Main(string[] args)
        {
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);
            
            var obj = new Buffering();

            try
            {
                string filename = @"Jul2-blockingcollection.txt";
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

                    obj.SendData(client, filename, path[i]);

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
