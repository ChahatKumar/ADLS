/*
 * Read 30 files ( 10 of 1 MB ,10 of 2MB,10 of 4 MB) via 5 buffers of 1MB ,5 buffers of 2MB and 5 Buffers of 4MB
 * in parallel using concurrent append in one adls file.
 */
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Buffers;

using Microsoft.Azure.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using System.Collections.Concurrent;
using System.Windows.Forms;

namespace Week4_2
{
    public class Buffering
    {
        private byte[][] buffer1 = new byte[5][];
        private byte[][] buffer2 = new byte[5][];
        private byte[][] buffer3 = new byte[5][];
        private ArrayPool<byte> samePool = ArrayPool<byte>.Shared;

        public Buffering()
        {
            for (int i = 0; i < 5; i++)
            {
                //renting 5 buffers of 1mb , 5 of 2 mb and 5 of 4 mb
                buffer1[i] = samePool.Rent(1050000);
                buffer2[i] = samePool.Rent(2100000);
                buffer3[i] = samePool.Rent(4200000);
            }
        }

        public void SendData(AdlsClient c, int i, string filename, string path)
        {
            if (i < 10)
            {
                i = i % 5;
                lock (buffer1[i])
                {
                    FileStream stream = File.OpenRead(path);
                    stream.Read(buffer1[i], 0, (int)stream.Length);
                    c.ConcurrentAppend(filename, true, buffer1[i], 0, (int)stream.Length);
                    Array.Clear(buffer1[i], 0, (int)stream.Length);
                    stream.Close();
                }
            }
            else if(i<20)
            {
                i = (i % 10) % 5;
                lock (buffer2[i])
                {
                    FileStream stream = File.OpenRead(path);
                    stream.Read(buffer2[i], 0, (int)stream.Length);
                    c.ConcurrentAppend(filename, true, buffer2[i], 0, (int)stream.Length);
                    Array.Clear(buffer2[i], 0, (int)stream.Length);
                    stream.Close();
                }
            }
             else 
            {
                i = (i % 10) % 5;
                lock (buffer3[i])
                {
                    FileStream stream = File.OpenRead(path);
                    stream.Read(buffer3[i], 0, (int)stream.Length);
                    c.ConcurrentAppend(filename, true, buffer3[i], 0, (int)stream.Length);
                    Array.Clear(buffer3[i], 0, (int)stream.Length);
                    stream.Close();
                }
            }
        }

        //destructor
        ~Buffering()
        {
            //returning all rented arrays 
            for (int i = 0; i < 5; i++)
            {
                samePool.Return(buffer1[i]);
                samePool.Return(buffer2[i]);
                samePool.Return(buffer3[i]);
            }
            Console.WriteLine("obj destroyed");
        }
    }
    public class Program
    {

        private static string applicationId = "35fe4d8f-b30e-40ed-8cdf-fee0216569de";     // Also called client id
        private static string clientSecret = ".P0KipHqZAj18k-.t8_-WN4v~90.Jst08h";
        private static string tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        private static string adlsAccountFQDN = "marstest.azuredatalakestore.net";   // full account FQDN, not just the account name like example.azure.datalakestore.net
                                                                                     // var samePool = ArrayPool<byte>.Shared;
                                                                                     // byte[] buffer = samePool.Rent(1000);

        public static void Main(string[] args)
        {
            // Obtain AAD token
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);
           
            var obj = new Buffering();

            try
            {
                //file that wil be created and appended to
                string filename = @"Output_10.txt";
                string[] path = new string[30];

                Parallel.For(0, 30, i => {
                    if (i < 10)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\1MB\" + ((i % 10) + 1) + ".txt";
                    }
                    else if(i<20)
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