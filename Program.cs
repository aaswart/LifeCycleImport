using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LifeCycleImport
{
  class Program
  {
    static async Task Main(string[] args)
    {
      Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
      Trace.WriteLine("Start import LifeCycle data");

      var pathFile = @"C:\temp\LC_export.csv";
      Trace.WriteLine($"File location:");
      Trace.WriteLine($"{pathFile}");

      Console.WriteLine("Press enter to start");
      Console.ReadLine();
      Console.WriteLine("Moment...");

      var conString = @"
Data Source=sql.sd.local;
Initial Catalog=HrSwartDevelopment;
User ID=u;
Password=u;
Connect Timeout=30;
Encrypt=False;
TrustServerCertificate=False;
ApplicationIntent=ReadWrite;
MultiSubnetFailover=False";
      var importTableName = "LC_export";
      var onlyImportFrom = DateTime.Parse("2020-01-01");
      using (var con = new SqlConnection(conString))
      {
        await con.OpenAsync();
        using (var trans = con.BeginTransaction())
        {
          try
          {
            using (var com = new SqlCommand())
            {
              com.Connection = con;
              com.Transaction = trans;
              com.CommandType = System.Data.CommandType.Text;
              com.CommandTimeout = 30;
              Trace.WriteLine($"Clear table '{importTableName}'");
              com.CommandText = $"DELETE FROM [{importTableName}]";
              await com.ExecuteNonQueryAsync();
              foreach (var row in ParseCsvWithHeader(pathFile))
              {
                if (com.CommandText.StartsWith("DELETE"))
                {
                  var sql = $"INSERT INTO [{importTableName}] (";
                  sql += row.Keys.Select(k => $"[{k.Trim()}]").Aggregate((x, y) => $"{x},{y}");
                  sql += ") VALUES(@p0";
                  for (int i = 1; i < row.Keys.Count; i++)
                    sql += $",@p{i}";
                  sql += ");";
                  com.CommandText = sql;
                  for (int i = 0; i < row.Keys.Count; i++)
                    com.Parameters.Add(new SqlParameter($"@p{i}", System.Data.SqlDbType.VarChar, 50));
                }
                var values = row.Values.ToArray();
                for (int i = 0; i < row.Keys.Count; i++)
                  com.Parameters[i].Value = values[i];
                                
                if ((DateTime.TryParse(row.Values.FirstOrDefault(), out var date)) && (date >= onlyImportFrom))
                {
                  Trace.WriteLine(row.Values.FirstOrDefault() + "*");
                  await com.ExecuteNonQueryAsync();
                }
                else
                  Trace.WriteLine(row.Values.FirstOrDefault() ?? "?");
              }
            }
            trans.Commit();
          }
          catch (Exception ex)
          {
            trans.Rollback();
            Trace.WriteLine(ex.ToString());
          }
        }
      }
    }

    public static IEnumerable<IDictionary<string, string>> ParseCsvWithHeader(string csvInput)
    {
      using (var csvReader = new StreamReader(csvInput))
      using (var parser = new NotVisualBasic.FileIO.CsvTextFieldParser(csvReader))
      {
        parser.CompatibilityMode = true;
        parser.SetDelimiter(',');
        if (parser.EndOfData)
        {
          yield break;
        }

        string[] headerFields = parser.ReadFields();
        while (!parser.EndOfData)
        {
          string[] fields = parser.ReadFields();
          int fieldCount = Math.Min(headerFields.Length, fields.Length);
          IDictionary<string, string> fieldDictionary = new Dictionary<string, string>(fieldCount);

          for (var i = 0; i < fieldCount; i++)
          {
            string headerField = headerFields[i];
            string field = fields[i];
            fieldDictionary[headerField] = field;
          }

          yield return fieldDictionary;
        }
      }
    }
  }
}
