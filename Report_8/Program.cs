/*
 *Read 20 files( 10 of 1 MB and 10 of 2 MB) in parallel with 5 buffers of 1MB and
 * 5 buffers of 2 MB using concurrent append in one file. 		
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

        // using blocking collections of type byte [] to act as a buffer pool with max capping of 5 for each 
        // configuration ( 1mb , 2 mb, 4 mb)
        private BlockingCollection<byte []> b1 = new BlockingCollection<byte []>(5);
        private BlockingCollection<byte []> b2 = new BlockingCollection<byte []>(5);

        // blocking collection to synchronize various sections
        private BlockingCollection<int> sync1 = new BlockingCollection<int>(1);
        private BlockingCollection<int> sync2 = new BlockingCollection<int>(1);

        // total initialed is the number of arrays that have been allocated in memmory so far 
        private int total_initialized_1MB  = 0;
        private int total_initialized_2MB = 0;

        //function to send data from path to target ( filename)
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

                    //if total initialised is less than 5 and buffer is empty -> we allocate new array
                    if( total_initialized_1MB <=4 && b1.Count==0)
                    {
                          byte [] temp = new byte[One_MB];
                          b1.Add(temp);
                          total_initialized_1MB++;
                        
                    }                
                   //taking array 
                   buffer1 = b1.Take();
                  
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
                 //returning the used array back to pool
                  b1.Add(buffer1);
                    
                }

                //sending data via 2 MB buffer if data is more than 1 MB but less than/ equal to 2 MB
                else 
                {
                  
                     sync2.Add(1);

                     byte[] buffer2;

                     //if total initialised is less than 5 and buffer is empty -> we allocate new array
                     if( total_initialized_2MB <=4 && b2.Count==0)
                     {
                          byte [] temp = new byte[Two_MB];
                          b2.Add(temp);
                          total_initialized_2MB++;  
                     }                
                     //taking array 
                     buffer2 = b2.Take();
                    
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
                     //returning the used array back to pool
                     b2.Add(buffer2);
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
                string filename = @"DataCheckAgain.txt";
                string[] path = new string[30];

                Parallel.For(0,20, i => {

                    if (i < 10)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\1MB\" + (i + 1) + ".txt";
                    }
                    else 
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\2MB\" + ((i % 10) + 1) + ".txt";
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