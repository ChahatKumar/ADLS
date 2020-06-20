/*
 * Read 10 files of 1 MB one after anther using concurrent append in one target adls file	
*/
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.Remoting.Contexts;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Azure.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using NLog.Internal;

namespace Report_4
{
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
            string fileName = @"Output_3.txt";

            try
            {
                string path;
                byte[] fileBytes = new Byte[1050000];

                for(int i=0;i<10;i++)
                {
                    
                    path = @"C:\Users\kchah\OneDrive\Desktop\_ADLS-master\Report_4\1MB\" + (i + 1) + ".txt";

                    // reading data
                    FileStream stream = File.OpenRead(path);

                    //storing to a byte array 
                    stream.Read(fileBytes, 0, fileBytes.Length);
                    client.ConcurrentAppend(fileName, true, fileBytes, 0, fileBytes.Length);

                    stream.Close();
                }
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