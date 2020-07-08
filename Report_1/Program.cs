/*
 *  Read 10 files of 1 MB one after another using normal append in one target adls file. 
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
        private static string applicationId = "35fe4d8f-b30e-40ed-8cdf-fee0216569de";     // Also called client id
        private static string clientSecret = ".P0KipHqZAj18k-.t8_-WN4v~90.Jst08h";
        private static string tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        private static string adlsAccountFQDN = "marstest.azuredatalakestore.net";   // full account 
        public static void Main(string[] args)
        {
            var creds = new ClientCredential(applicationId, clientSecret);
            var clientCreds = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).GetAwaiter().GetResult();
            // Create ADLS client object
            AdlsClient client = AdlsClient.CreateClient(adlsAccountFQDN, clientCreds);

            try
            {
                //file that wil be created and appended to 
                string fileName = @"hellloo.txt";

                // file from which data is read
                string path;
                byte[] fileBytes= new Byte[1050000];
                
                using (var Stream = client.CreateFile(fileName, IfExists.Overwrite))
                {
                    // file will be created/overwritten 
                }


                //looping over all input files to append data to target file 
                for (int i = 0; i < 1; i++)
                {
                    path = @"C:\Users\kchah\OneDrive\Desktop\InputFiles\4MB\" + (i + 1) + ".txt";
                    //reading from path
                    FileStream stream = File.OpenRead(path);
                    stream.Read(fileBytes, 0, fileBytes.Length);
                    stream.Close();

                    using (var Stream = client.GetAppendStream(fileName))
                    {

                        //appending data from path
                        Stream.Write(fileBytes, 0, fileBytes.Length);
                    }
                    Array.Clear(fileBytes, 0, fileBytes.Length);

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
