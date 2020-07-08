/*
 *  Read 10 files of 1 MB in parallel using normal append in 10 output adls files.
*/

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;

namespace Report_1
{
    public class Program
    {
        private static string applicationId = "35fe4d8f-b30e-40ed-8cdf-fee0216569de";  // Also called client id
        private static string clientSecret = ".P0KipHqZAj18k-.t8_-WN4v~90.Jst08h";
        private static string tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        private static string adlsAccountFQDN = "marstest.azuredatalakestore.net";   // full account FQDN( not just the account name) like example.azure.datalakestore.net

        public static void Main(string[] args)
        {
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);

            try
            {
               
                string[] fileName = new string[10];
                // Creating outside Parallel for loop so that it does not interfere with 
                // individual latency check for append operation 
                for (int i = 0; i < 10; i++)
                {
                    fileName[i] = @"Output_2_subbb"+ (i+1) + ".txt";
                    using (var Stream = client.CreateFile(fileName[i], IfExists.Overwrite))
                    {
                        // file will be created/overwritten 
                    }
                }
                string[] path = new string[10];
                byte[][] fileBytes = new byte[10][];

                Parallel.For(0,10,i=>{
                   

                    path[i] = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\1MB\"+ (i + 1) + ".txt";
                    FileStream stream = File.OpenRead(path[i]);
                    fileBytes[i] = new byte[stream.Length];

                    stream.Read(fileBytes[i], 0, fileBytes[i].Length);
                    stream.Close();

                    using (var Stream = client.GetAppendStream(fileName[i]))
                    {
                        //appending data from path
                        Stream.Write(fileBytes[i], 0, fileBytes[i].Length);
                    }

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
