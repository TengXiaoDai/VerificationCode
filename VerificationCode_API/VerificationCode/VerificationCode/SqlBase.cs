using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DBHelper
{
    public class SqlBase
    {
        /// <summary>
        /// string-字段值,object-图片信息
        /// </summary>
        public Dictionary<string, object> ImageList = new Dictionary<string, object>();
        SqlConnection con;

        public static SqlConnection getCon()
        {
            string strConString = System.Configuration.ConfigurationManager.ConnectionStrings["ConnStringSQL"].ConnectionString;
            SqlConnection con = new SqlConnection(strConString);
            return con;
        }

        /// <summary>
        /// 执行SQL语句填充dataset
        /// </summary>
        /// <param name="sql">查询语句</param>
        /// <returns></returns>
        public static DataSet FillDataSet(string sql)
        {
            DataSet ds = new DataSet();
            SqlConnection conn = getCon();
            try
            {
                conn.Open();
                SqlCommand sqlCommd = new SqlCommand(sql, conn);
                SqlDataAdapter adapter = new SqlDataAdapter(sqlCommd);
                adapter.Fill(ds);
                conn.Close();
                return ds;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + " SQL：" + sql);
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 执行无返回数据表的sql语句
        /// </summary>
        /// <returns></returns>
        public static bool UpdateData(string sql)
        {
            SqlConnection conn = getCon();
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + " SQL：" + sql);
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 执行无返回并插入系统日志数据表的sql语句
        /// </summary>
        /// <returns></returns>
//        public static bool UpdateDataAndLog(string sql, string OperateName)
//        {
//            SqlConnection conn = getCon();
//            SqlCommand cmd = new SqlCommand(sql, conn);
//            try
//            {
//                conn.Open();
//                cmd.ExecuteNonQuery();

//                #region 写入日志

//                string type = "1";
//                if (sql.ToUpper().Contains("INSERT") || sql.Contains("UPDATE"))
//                {
//                    type = "1";
//                }
//                else
//                {
//                    type = "2";//删除
//                }
//                string insSql = string.Format(@"INSERT INTO dbo.SYS_Log (Log_Code
//                                    , Log_Type
//                                    , Log_Sql
//                                    , Log_UserName)
//                                    Values(
//                                    '{0}',{1},'{2}','{3}'
//                                    )", GetCode(), type, sql, OperateName);
//                UpdateData(insSql);

//                #endregion

//                return true;
//            }
//            catch (Exception ex)
//            {
//                throw new Exception(ex.Message + " SQL：" + sql);
//            }
//            finally
//            {
//                conn.Close();
//            }
//        }

        /// <summary>
        /// 执行无返回数据表的sql语句,带1个图片数据流
        /// </summary>
        /// <returns></returns>
        public static bool UpdateData(string sql, byte[] ImageData)
        {
            SqlConnection conn = getCon();
            SqlCommand cmd = new SqlCommand(sql, conn);
            try
            {
                conn.Open();
                if (ImageData != null)
                {
                    cmd.Parameters.Add("@Data", SqlDbType.Image);
                    cmd.Parameters["@Data"].Value = ImageData;
                }
                cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + " SQL：" + sql);
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 返回DataTable数据集
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static DataTable SearchData(string sql)
        {
            SqlConnection conn = getCon();
            try
            {
                conn.Open();
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                SqlDataAdapter da = new SqlDataAdapter();
                DataSet ds = new DataSet();
                da.SelectCommand = cmd;
                da.Fill(ds);
                return ds.Tables[0];
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + " SQL：" + sql);
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 得到table数据源
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static DataRow GetTableRow(string sql)
        {
            SqlConnection conn = getCon();
            try
            {
                SqlCommand cmd = new SqlCommand(sql, conn);
                SqlDataAdapter da = new SqlDataAdapter();
                DataSet ds = new DataSet();
                da.SelectCommand = cmd;
                da.Fill(ds);

                if (ds != null && ds.Tables[0].Rows.Count > 0)
                {
                    return ds.Tables[0].Rows[0];
                }
                return null;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + " SQL：" + sql);
            }
            finally
            {
                conn.Close();
            }
        }

        /// <summary>
        /// 增加Table带序号列
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="IndexName"></param>
        public static void AddRowIndex(DataTable dt, string IndexName)
        {
            DataColumn dc = new DataColumn(IndexName);
            dt.Columns.Add(dc);

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                dt.Rows[i][IndexName] = i + 1;
            }
        }

        //系统编码
//        public static string GetCode()
//        {
//            return ValueHandler.GetStringValue(SearchData(@"DECLARE @NCode nvarchar(20)
//                        exec dbo.SP_GetCode @NCode output
//                        select @NCode").Rows[0][0]);
//        }

        /// <summary>
        /// 使用SqlBulkCopy 提交DataTable到数据
        /// </summary>
        /// <param name="dt">数据源</param>
        /// <param name="tableName">表名</param>
        /// <returns></returns>
        public static bool ExecuteInsert(DataTable dt, string tableName)
        {
            using (SqlConnection con = getCon())
            {
                try
                {
                    con.Open();
                    using (SqlBulkCopy sqlbulkcopy = new SqlBulkCopy(con))
                    {
                        //DataTable schema = new DataTable();
                        //schema = con.GetSchema();

                        sqlbulkcopy.DestinationTableName = tableName;
                        sqlbulkcopy.BulkCopyTimeout = 18000;
                        sqlbulkcopy.WriteToServer(dt);

                        return true;
                    }
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message);
                }
                finally
                {
                    con.Close();
                }
            }
        }

        /// <summary>
        /// 执行带参数的无返回数据的存储过程
        /// </summary>
        /// <param name="SpName"></param>
        /// <param name="parm"></param>
        /// <returns></returns>
        public static bool ExcuteNonQuery_Sp(string SpName, out int intResult, SqlParameter[] parms = null)
        {
            SqlConnection conn = getCon();
            SqlCommand cmd = new SqlCommand(SpName, conn);

            cmd.CommandType = CommandType.StoredProcedure;
            if (parms!=null)
            {
                foreach (SqlParameter parm in parms)
                    cmd.Parameters.Add(parm);
            }
            try
            {
                conn.Open();
                //PrepareCommand(cmd, conn, null, CommandType.StoredProcedure, SpName, parms);
                cmd.ExecuteNonQuery();
                intResult = 0;
                return true;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        public static DataTable ExcuteNonQuery_Sp(string SpName, SqlParameter[] parms=null)
        {
            SqlConnection conn = getCon();
            SqlCommand cmd = new SqlCommand(SpName, conn);

            cmd.CommandType = CommandType.StoredProcedure;
            if (parms!=null)
            {
                foreach (SqlParameter parm in parms)
                    cmd.Parameters.Add(parm); 
            }
            try
            {
                conn.Open();
                //PrepareCommand(cmd, conn, null, CommandType.StoredProcedure, SpName, parms);
                SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                return ds.Tables[0];
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
