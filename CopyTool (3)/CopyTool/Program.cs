using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CopyTool
{
    class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            String logName = DateTime.Now.ToString("yyyyMMddHHmmss");

            FolderBrowserDialog fdb = new FolderBrowserDialog();
            fdb.Description = "Select the destination folder for copying the files";
            fdb.SelectedPath = @"Y:\FairfaxStorage\Images\Exported Images";
            if (fdb.ShowDialog() != DialogResult.OK)
            {
                Environment.Exit(0);
            }
            String strDestinationRoot = fdb.SelectedPath;

            try
            {
                SqlConnection cnnDB = new SqlConnection();
                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Connecting to database");
                String strCnnString = String.Format("Data Source=USDXYSMRL1VW024;Initial Catalog=MigrationDB;User ID=FFXUser;Password=Fairfax1");
                cnnDB.ConnectionString = strCnnString;
                cnnDB.Open();

                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Database connected.");

                String strSql = String.Format("select DocId, FetchExportDirectory from is_docmap where MimeType != 'image/tiff' and FetchStatus = 'Success' and MergeStatus is null and FetchExportDirectory is not null");
                DataSet ds = new DataSet();
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = new SqlCommand(strSql, cnnDB);
                da.SelectCommand = cmd;
                da.Fill(ds);
                DataRowCollection drSourceDocs = ds.Tables[0].Rows;
                cnnDB.Close();

                int totalDoc = drSourceDocs.Count;
                int successDoc = 0;
                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Found " + totalDoc + " docs ready to copy");

                foreach(DataRow drSourceDoc in drSourceDocs)
                {
                    try
                    {
                        String strDocID = drSourceDoc[0].ToString();
                        String strSourceFolder = drSourceDoc[1].ToString();
                        String strDestinationFolder = strDestinationRoot + @"\" + strDocID;
                        Directory.CreateDirectory(strDestinationFolder);

                        Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Start copying doc " + strDocID + ".");

                        foreach(String newPath in Directory.GetFiles(strSourceFolder))
                        {
                            File.Copy(newPath, newPath.Replace(strSourceFolder, strDestinationFolder), true);
                        }

                        int sourceCount = Directory.GetFiles(strSourceFolder).Count();
                        int destinationCount = Directory.GetFiles(strDestinationFolder).Count();

                        if (sourceCount != destinationCount)
                            continue;

                        strSql = String.Format("update MigrationDB.dbo.is_docmap set MergeStatus = 'Success', MergeExportDirectory = '{0}' where DocId = {1}", strDestinationFolder, strDocID);
                        cnnDB.Open();
                        cmd = new SqlCommand();
                        SqlTransaction trans = cnnDB.BeginTransaction();
                        cmd.Connection = cnnDB;
                        cmd.Transaction = trans;
                        cmd.CommandText = strSql;
                        cmd.ExecuteNonQuery();
                        trans.Commit();
                        cnnDB.Close();

                        successDoc++;
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine("Error inside the loop");
                        continue;
                    }
                }

            }
            catch (SqlException) { Console.WriteLine("SQL Connection Error"); }
            catch (Exception) { }

            Console.WriteLine("Copying finished.");
            Console.ReadKey();
        }
    }
}
