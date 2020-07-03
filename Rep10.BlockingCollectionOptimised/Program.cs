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

        private BlockingCollection<byte []> b1 = new BlockingCollection<byte []>(5);
        private BlockingCollection<byte []> b2 = new BlockingCollection<byte []>(5);
        private BlockingCollection<byte []> b4 = new BlockingCollection<byte []>(5);

        private BlockingCollection<int> sync1 = new BlockingCollection<int>(1);
        private BlockingCollection<int> sync2 = new BlockingCollection<int>(1);
        private BlockingCollection<int> sync4 = new BlockingCollection<int>(1);

        private int total_initialized_1MB  = 0;
        private int total_initialized_2MB = 0;
        private int total_initialized_4MB  = 0;

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

                    byte[] buffer1;

                    if( total_initialized_1MB <=4)
                    {
                        //if array present in bufferpool and total allocations <= 4
                        if(b1.Count!=0)
                        {
                          buffer1 = b1.Take();
                        }
                        //allocating new array
                        else
                        {
                          byte [] temp = new byte[One_MB];
                          b1.Add(temp);
                          buffer1 = b1.Take();
                          total_initialized_1MB++;
                        }
                    }                
                    else
                    {
                        buffer1 = b1.Take();
                    }
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

                 b1.Add(buffer1);
                    
                }

                //sending data via 2 MB buffer if data is more than 1 MB but less than/ equal to 2 MB
                else if(length - Two_MB <= 0)
                {
                  
                     sync2.Add(1);

                     byte[] buffer2;
                     if( total_initialized_2MB <=4)
                     {
                        //if array present in bufferpool and total allocations <= 4
                        if(b2.Count!=0)
                        {
                           buffer2 = b2.Take();
                        }
                        //allocating new array
                        else
                        {
                          byte [] temp = new byte[Two_MB];
                          b2.Add(temp);
                          buffer2 = b2.Take();
                          total_initialized_2MB++;
                        }
                     }                
                     else
                     {
                        buffer2 = b2.Take();
                     }
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
                     b2.Add(buffer2);
                }
                //sending data via 4 MB if data is more than 2 MB
                else
                {
                    sync4.Add(1);

                    byte[] buffer4;
                    if( total_initialized_4MB <=4)
                    {
                     
                        //if array present in bufferpool and total allocations <= 4
                        if(b4.Count!=0)
                        {
                           buffer4 = b4.Take();
                        }
                       //allocating new array
                        else
                        {
                          byte [] temp = new byte[Four_MB];
                          b4.Add(temp);
                          buffer4 =  b4.Take();
                          total_initialized_4MB++;
                        }
                    }                
                    else
                    {
                        buffer4 = b4.Take();
                    }
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
                    b4.Add(buffer4);
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
                string filename = @"Optimised.txt";
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
