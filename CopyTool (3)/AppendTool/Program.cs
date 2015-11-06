using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppendTool
{
    class Program
    {
        public static String startTime;
        public static String logPath;
        public static String customErrMesg;
        public static List<String> lines;

        static void Main(string[] args)
        {
            startTime = DateTime.Now.ToString("yyyyMMddHHmmss");
            logPath = @"C:\log";

            SqlConnection cnnDB = new SqlConnection();
            try
            {
                String strCnnString = String.Format("Data Source=USDXYSMRL1VW024;Initial Catalog=MigrationDB;User ID=FFXUser;Password=Fairfax1");
                cnnDB.ConnectionString = strCnnString;
                cnnDB.Open();
                cnnDB.Close();
                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Database connected.");

                String strSql = "select [Doc ID], [SAP object ID], [Doc type ] from filenet where ImportStatus is null";
                DataRowCollection drFileNetDocIDs = dbSelect(strSql, cnnDB).Tables[0].Rows;
                int total = drFileNetDocIDs.Count;
                int left = total;
                int successed = 0;
                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Found records: " + total);

                foreach (DataRow drFileNetDocID in drFileNetDocIDs)
                {
                    try
                    {
                        left--;

                        String strDocID = drFileNetDocID[0].ToString();
                        String strSAP = drFileNetDocID[1].ToString();
                        String strDocType = drFileNetDocID[2].ToString();
                        Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Processing " + strDocID);

                        strSql = "select SAPObjectID, DocType from PhilipsIS where DocId = " + strDocID;
                        DataRowCollection drPhilipsISDocID = dbSelect(strSql, cnnDB).Tables[0].Rows;
                        if (drPhilipsISDocID.Count == 0)
                            continue;
                        String strSAPText = drPhilipsISDocID[0][0].ToString();
                        String strDocTypeText = drPhilipsISDocID[0][0].ToString();

                        if (!String.IsNullOrEmpty(strSAP) && !strSAPText.Contains(strSAP))
                            strSAPText += strSAP + ", ";
                        if (!String.IsNullOrEmpty(strDocType) && !strDocTypeText.Contains(strDocType))
                            strDocTypeText += strDocType + ", ";
                        strSql = "update PhilipsIS set SAPObjectID = " + strSAPText + ", DocType = " + strDocTypeText + " where DocId = " + strDocID;
                        dbTransaction(strSql, cnnDB);

                        strSql = "update filenet set ImportStatus = 'Success' where [Doc ID] = '" + strDocID + "'";
                        dbTransaction(strSql, cnnDB);

                        successed++;
                        Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Successed " + successed + " Failed");
                        Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Left " + left);

                        if (successed % 100 == 0)
                            Console.Title = DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + " => Left " + left;
                    }
                    catch(Exception e)
                    {
                        lines.Clear();
                        lines.Add(e.Message);
                        logging(lines);
                    }
                }
            }
            catch (SqlException e)
            {
                Console.WriteLine("*****SQL ERROR*****");
            }
            catch (Exception e)
            {
                Console.WriteLine("*****Oops, Unexpected ERROR*****");
            }
        }

        private static void logging(List<String> mesgs)
        {
            logPath += @"\" + startTime + ".txt";
            if (!File.Exists(logPath))
            {
                using (StreamWriter file = File.CreateText(logPath))
                {
                    file.WriteLine("Tool started at " + startTime);
                }
            }
            using (StreamWriter file = File.AppendText(logPath))
            {
                String str = DateTime.Now.ToString();
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
                logging(lines);
                throw;
            }
            finally
            {
                cnn.Close();
            }

            return ds;
        }

        private static void dbTransaction(String sql, SqlConnection cnn)
        {
            cnn.Open();
            SqlCommand cmd = new SqlCommand();
            SqlTransaction trans = cnn.BeginTransaction();

            try
            {
                cmd.Connection = cnn;
                cmd.Transaction = trans;
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();

                trans.Commit();
            }
            catch (Exception e)
            {
                trans.Rollback();
                customErrMesg = String.Format(@"transaction issue for query: <{0}>", sql);
                lines.Clear();
                lines.Add(customErrMesg);
                lines.Add(e.Message);
                logging(lines);
                throw;
            }
            finally
            {
                cnn.Close();
            }
        }
    }
}
