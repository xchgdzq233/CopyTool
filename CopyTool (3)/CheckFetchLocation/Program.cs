using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CheckFetchLocation
{
    class Program
    {
        public static String customErrMesg;
        public static String startTime;
        public static List<String> lines;

        static void Main(string[] args)
        {
            String strFolderRoot = @"Y:\FairfaxStorage\Images\";
            int intMaxFolder = 11;
            String reportPath = @"c:\Reports\";
            String strMovePath = @"Y:\FairfaxStorage\Images\DuplicateSinglePageTiff";

            startTime = DateTime.Now.ToString("yyyyMMddHHmmss");
            reportPath = Path.Combine(reportPath, startTime + ".txt");
            int totalDocs = 0;
            int totalMatchDocs = 0;
            lines = new List<string>();

            SqlConnection cnnDB = new SqlConnection();
            try
            {
                String strCnnString = String.Format("Data Source=USDXYSMRL1VW024;Initial Catalog=MigrationDB;User ID=FFXUser;Password=Fairfax1");
                cnnDB.ConnectionString = strCnnString;
                cnnDB.Open();
                cnnDB.Close();
                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Database connected.");

                for (int i = 0; i <= intMaxFolder; i++)
                {
                    String strFolder = Path.Combine(strFolderRoot, i.ToString());
                    if (!Directory.Exists(strFolder))
                    {
                        continue;
                    }
                    Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Accessing folder " + i);
                    List<String> lstDocFolders = Directory.GetDirectories(strFolder).Select(docName => Path.GetFileName(docName)).ToList();
                    int totalSubDocs = lstDocFolders.Count();
                    int totalSubMatchDocs = 0;
                    totalDocs += totalSubDocs;
                    Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Got " + totalSubDocs + " of folders");
                    foreach (String docID in lstDocFolders)
                    {
                        String strSql = "select DOCS_Pages, FetchExportDirectory from is_docmap where DocId = '" + docID + "'";
                        DataRow drDoc = dbSelect(strSql, cnnDB).Tables[0].Rows[0];
                        int intPage = Convert.ToInt32(drDoc[0]);
                        String strDBFolderPath = drDoc[1].ToString();
                        String strFolderPath = Path.Combine(strFolder, docID);
                        if (strDBFolderPath.Equals(strFolderPath))
                        {
                            totalSubMatchDocs++;
                        }
                        else
                        {
                            lines.Clear();
                            lines.Add(docID + " with " + intPage + " pages");
                            lines.Add("DBPath: " + strDBFolderPath);
                            lines.Add("ActualPath: " + strFolderPath);
                            logging(lines, reportPath);

                            Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Start moving...");
                            int version = 0;
                            String strTargetDoc = Path.Combine(strMovePath, docID + "." + version);
                            while (Directory.Exists(strTargetDoc))
                            {
                                strTargetDoc = Path.Combine(strMovePath, docID + "." + (++version));
                            }
                            Directory.CreateDirectory(strTargetDoc);
                            List<String> strSourceFiles = Directory.GetFiles(strFolderPath).ToList();
                            foreach (String strSourceFile in strSourceFiles)
                            {
                                new FileInfo(strSourceFile).MoveTo(Path.Combine(strTargetDoc, Path.GetFileName(strSourceFile)));
                            }
                            Directory.Delete(strFolderPath);

                            Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Moving to " + strTargetDoc + " done.");
                            lines.Clear();
                            lines.Add("Files moved to " + strTargetDoc);
                            logging(lines, reportPath);
                        }
                    }
                    totalMatchDocs += totalSubMatchDocs;
                    lines.Clear();
                    lines.Add("Found " + totalSubDocs + " of docs under folder " + i);
                    lines.Add("Match: " + totalSubMatchDocs + ". Unmatch: " + (totalSubDocs - totalSubMatchDocs));
                    logging(lines, reportPath);
                }
                lines.Clear();
                lines.Add("CheckTool finished. Found " + totalDocs);
                lines.Add("Match: " + totalMatchDocs + ". Unmatch: " + (totalDocs - totalMatchDocs));
                logging(lines, reportPath);
            }
            catch (SqlException e)
            {
                Console.WriteLine("SQL error");
            }
            catch (Exception e)
            {
                Console.WriteLine("Unexcepted error");
            }
            finally
            {
                cnnDB.Dispose();
            }
            Console.ReadKey();
        }

        private static void logging(List<String> mesgs, String logPath)
        {
            if (!File.Exists(logPath))
            {
                using (StreamWriter file = File.CreateText(logPath))
                {
                    file.WriteLine("tool started at " + startTime.ToString());
                }
            }
            using (StreamWriter file = File.AppendText(logPath))
            {
                String str = Environment.NewLine + DateTime.Now.ToString();
                foreach (String mesg in mesgs)
                {
                    str += System.Environment.NewLine + mesg;
                }
                file.WriteLine(str);
                Console.WriteLine(str);
            }
        }

        private static DataSet dbSelect(String sql, SqlConnection cnn)
        {
            DataSet ds = new DataSet();
            SqlDataAdapter da = new SqlDataAdapter();
            SqlCommand cmd = new SqlCommand();

            try
            {
                cnn.Open();
                cmd = new SqlCommand(sql, cnn);

                da.SelectCommand = cmd;
                da.Fill(ds);
            }
            catch (Exception e)
            {
                customErrMesg = String.Format(@"select issue for query: <{0}>", sql);
                lines.Clear();
                lines.Add(customErrMesg);
                lines.Add(e.Message);
                logging(lines, @"C:\log\" + startTime + ".txt");
                throw;
            }
            finally
            {
                cnn.Close();
            }

            return ds;
        }
    }
}
