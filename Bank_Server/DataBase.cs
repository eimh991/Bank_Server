using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bank_Server
{
    public class DataBase
    {
        private static string connString = ConfigurationManager.ConnectionStrings["ConnString"].ConnectionString;

        private static SqlConnection connection = new SqlConnection(connString);

        public void OpenConnection()
        {
            if (connection.State == System.Data.ConnectionState.Closed)
            {
                connection.Open();
            }
        }

        public void CloseConnection()
        {
            if (connection.State == System.Data.ConnectionState.Open)
            {
                connection.Close();
            }
        }
        public SqlConnection Connection { get { return connection; } }

    }
}

