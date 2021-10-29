using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Common;
using System.Configuration;

namespace dMapper
{
    public class ConnectionProvider
    {
        public static ConnectionInfo GetConnectionInfo(string strConnName)
        {
            //Instatiate class for return connection information.
            ConnectionInfo connInfo = new ConnectionInfo();

            //Populate data.
            connInfo.ConnectionString = ConfigurationManager.ConnectionStrings[strConnName].ConnectionString;
            connInfo.ProviderName = ConfigurationManager.ConnectionStrings[strConnName].ProviderName;

            //Return connection information.
            return connInfo;
        }

        public static DbConnection GetConnection()
        {
            return GetConnection(null);
        }

        public static DbConnection GetConnection(string strConnName)
        {
            string internalStrConnName = (string.IsNullOrEmpty(strConnName) ? ConnectionName.ConnStr.ToString() : strConnName);

            //Return connection information.
            ConnectionInfo connInfo = GetConnectionInfo(internalStrConnName);
            
            //Get factory for connection provider.
            DbProviderFactory factory = DbProviderFactories.GetFactory(connInfo.ProviderName);
            
            //Create connection.
            DbConnection conn = factory.CreateConnection();

            //ConnectionString.
            conn.ConnectionString = connInfo.ConnectionString;

            //Open connection.
            conn.Open();

            //Return connection.
            return conn;
        }

        public static ProviderName GetProviderName(string strConnName)
        {
            ProviderName provider;

            string internalStrConnName = (string.IsNullOrEmpty(strConnName) ? ConnectionName.ConnStr.ToString() : strConnName);

            string providerNameConnStr = ConfigurationManager.ConnectionStrings[internalStrConnName].ProviderName;

            if (providerNameConnStr == "System.Data.SqlClient")
            {
                provider = ProviderName.SystemDataSqlClient;
            }
            else if (providerNameConnStr == "MySql.Data.MySqlClient")
            {
                provider = ProviderName.MySqlDataMySqlClient;
            }
            else
            {
                throw new ArgumentException("Provider não suportado.");
            }

            return provider;
        }
    }
}
