/*
 * Read 20 files ( 10 of 1 MB and 10 of 2MB) in parallel using concurrent append in one adls file.
 */
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using NLog.Internal;

namespace Week3_1
{
    public class Program
    {
        private static string applicationId = "35fe4d8f-b30e-40ed-8cdf-fee0216569de";     // Also called client id
        private static string clientSecret = ".P0KipHqZAj18k-.t8_-WN4v~90.Jst08h";
        private static string tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        private static string adlsAccountFQDN = "marstest.azuredatalakestore.net";   // full account FQDN, not just the account name like example.azure.datalakestore.net

        public static void Main(string[] args)
        {
            // Obtain AAD token
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);
            string fileName = @"Output_7.txt";

            try
            {
             
                string[] path = new string[20];
                byte[][] fileBytes = new byte[20][];
                Parallel.For(0, 20, i =>
                {
                    if (i < 10)
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\1MB\" + (i + 1) + ".txt";

                    }
                    else
                    {
                        path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\2MB\" + ((i%10) + 1) + ".txt";

                    }

                    // reading data
                    FileStream stream = File.OpenRead(path[i]);
                    fileBytes[i] = new byte[stream.Length];

                    stream.Read(fileBytes[i], 0, fileBytes[i].Length);
                    client.ConcurrentAppend(fileName, true, fileBytes[i], 0, fileBytes[i].Length);

                    stream.Close();
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