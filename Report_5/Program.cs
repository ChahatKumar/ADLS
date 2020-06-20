/*
 *Read 10 files of 1 MB in parallel with buffer of 1MB using concurrent append in one file		 
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

namespace SingleBuffer
{
    public class Buffering
    {
        private byte[] buffer;
        private ArrayPool<byte> samePool = ArrayPool<byte>.Shared;

        /*-------*/
        public Buffering()
        {
            //renting 1 buffer of 1 mb
            buffer = samePool.Rent(1050000);
        }
        public void SendData(AdlsClient c, int i, string filename, string path)
        {
            lock (buffer)
            {
                FileStream stream = File.OpenRead(path);
                stream.Read(buffer, 0, (int)stream.Length);
                c.ConcurrentAppend(filename, true, buffer, 0, (int)stream.Length);
                Array.Clear(buffer, 0, (int)stream.Length);
                stream.Close();
            }
        }

        //destructor
        ~Buffering()
        {
            //returning the buffer
            samePool.Return(buffer);

            Console.WriteLine("obj destroyed");
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
                //file that wil be created and appended to
                string filename = @"Output_5.txt";
                string[] path = new string[10];
              
                Parallel.For(0, 10, i => {

                    path[i] = @"C:\Users\kchah\OneDrive\Desktop\_ADLS-master\Report_4\1MB\" + (i + 1) + ".txt";

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
