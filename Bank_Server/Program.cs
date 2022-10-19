using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Transactions;

namespace Bank_Server
{
    internal class Program
    {

        static TcpListener listener;
        static DataBase dataBase ;
        static string answer ="";
        static CommittableTransaction tx;

        static void Main(string[] args)
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
            int port = 10000;
            
           listener = new TcpListener(IPAddress.Parse("192.168.1.103"), port);

            listener.Start();
            Console.WriteLine("Сервер запущен");
            ThreadStart start = new ThreadStart(WorkServer);
            Thread thread = new Thread(start);
            thread.Start();
            //WorkServer();
            Console.ReadKey();






        }
        public static void WorkServer()
        {
            try
            {
                while (true)
                {
                    dataBase = new DataBase();

                    TcpClient client = listener.AcceptTcpClient();

                    NetworkStream stream = client.GetStream();

                    BinaryReader reader = new BinaryReader(stream);
                   
                    string message = reader.ReadString();

                    string[] messages = message.Split(';');

                    if(messages.Length == 2 && messages[0]== "Show")
                    {
                        string query = $"SELECT Balance FROM CLient WHERE Id ={Convert.ToInt32(messages[1])}";

                        dataBase.OpenConnection();

                        SqlCommand cmd = new SqlCommand(query, dataBase.Connection);

                        var dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        var rows = dt.AsEnumerable().ToArray();
                        foreach (DataRow raw in rows)
                        {
                            answer = raw.ItemArray[0].ToString();
                        }

                        BinaryWriter writer = new BinaryWriter(stream);
                        writer.Write(answer);
                        writer.Flush();

                        reader.Close();
                        stream.Close();
                        writer.Close();
                        dataBase.CloseConnection();
                    }
                    else if(messages.Length == 3 && messages[0] == "BalanceAdd")
                    {
                        string query = $"UPDATE CLient SET Balance += {float.Parse(messages[2])} WHERE Id ={Convert.ToInt32(messages[1])}";

                        dataBase.OpenConnection();

                        SqlCommand cmd = new SqlCommand(query, dataBase.Connection);

                        cmd.ExecuteNonQuery();
                        answer = $"Баланс пополнен на {float.Parse(messages[2])}";
                        BinaryWriter writer = new BinaryWriter(stream);
                        writer.Write(answer);
                        writer.Flush();

                        reader.Close();
                        stream.Close();
                        writer.Close();
                        dataBase.CloseConnection();

                    }
                    else if(messages.Length == 4 && messages[0] == "Transfer")
                    {
                        try
                        {
                            tx = new CommittableTransaction();
                            if (!IsExistId(Convert.ToInt32(messages[3])))
                            {
                                NullReferenceException ex = new NullReferenceException();
                                throw (ex);
                            }
                            dataBase.OpenConnection();
                            dataBase.Connection.EnlistTransaction(tx);
                            string query = $"UPDATE CLient SET Balance -= {float.Parse(messages[2])} WHERE Id ={Convert.ToInt32(messages[1])}";
                            SqlCommand cmd = new SqlCommand(query,dataBase.Connection);
                            cmd.ExecuteNonQuery();
                            query = $"UPDATE CLient SET Balance += {float.Parse(messages[2])} WHERE Id ={Convert.ToInt32(messages[3])}";
                            cmd = new SqlCommand(query, dataBase.Connection);
                            cmd.ExecuteNonQuery();
                            tx.Commit();
                            answer = $"Перевод на сумму {float.Parse(messages[2])} пользвателю  с Id = {Convert.ToInt32(messages[3])} осуществелн";
                            BinaryWriter writer = new BinaryWriter(stream);
                            writer.Write(answer);
                            writer.Flush();

                            reader.Close();
                            stream.Close();
                            writer.Close();
                            dataBase.CloseConnection();

                        }
                        catch(NullReferenceException ex)
                        {
                            tx.Rollback();
                            answer = $"Перевод не выполнен,вы осуществяете перевод на несущестующий id";
                            BinaryWriter writer = new BinaryWriter(stream);
                            writer.Write(answer);
                            writer.Flush();
                            writer.Close();
                            dataBase.CloseConnection();
                        }
                        catch (System.Data.SqlClient.SqlException ex)
                        {
                            tx.Rollback();
                            answer = $"Перевод не выполнен, недостаточно средств";
                            BinaryWriter writer = new BinaryWriter(stream);
                            writer.Write(answer);
                            writer.Flush();
                            writer.Close();
                            dataBase.CloseConnection();

                        }
                        catch(Exception ex)
                        {
                            tx.Rollback();
                            answer = $"Ошибка перевода";
                            BinaryWriter writer = new BinaryWriter(stream);
                            writer.Write(answer);
                            writer.Flush();
                            writer.Close();
                            dataBase.CloseConnection();
                        }
                        finally
                        {  if(tx != null)
                            tx = null;
                        }
                    }

                }
            }catch(Exception ex)
            {
                
            }
            finally
            {
                
            }

        }
        public static bool IsExistId(int id)
        {
            dataBase.OpenConnection();
            string query = $"SELECT CLient.Id  FROM CLient";
            SqlCommand command = new SqlCommand(query, dataBase.Connection);
            SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                for(int i =0;i<reader.FieldCount;i++)
                {
                    if(Convert.ToInt32(reader[i].ToString())== id)
                    {
                        dataBase.CloseConnection();
                        return true;
                    }
                }
            }
            reader.Close();
            dataBase.CloseConnection();
            return false;

        }


    }
}
