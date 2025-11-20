using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Npgsql;

class Program
{
    private static readonly string WatchFolder =
        @"C:\Users\Promantus\Desktop\Kdbill\Xml_Files";

    private static readonly string ConnectionString =
        "Host=localhost;Port=5432;Username=postgres;Password=root;Database=CamtDB";

    static async Task Main()
    {
        Console.WriteLine($"📂 Watching folder: {WatchFolder}");

        if (!Directory.Exists(WatchFolder))
            Directory.CreateDirectory(WatchFolder);

        var xmlFiles = Directory.GetFiles(WatchFolder, "*.xml");
        foreach (var file in xmlFiles)
            await ProcessXmlFileAsync(file);

        Console.WriteLine("✅ Initial scan complete.");
        Console.ReadLine();
    }

    private static async Task ProcessXmlFileAsync(string filePath)
    {
        string fileName = Path.GetFileName(filePath);
        Console.WriteLine($"🔍 Processing XML: {fileName}");

        try
        {
            string xmlContent = await File.ReadAllTextAsync(filePath);
            var xml = XDocument.Parse(xmlContent);

            XNamespace ns = xml.Root?.GetDefaultNamespace() ?? XNamespace.None;

            // Extract CAMT Group Header fields
            string msgId = xml.Descendants(ns + "MsgId").FirstOrDefault()?.Value ?? "";
            string msgRcptName = xml.Descendants(ns + "Nm").FirstOrDefault()?.Value ?? "";
            string msgRcptOrgId = xml.Descendants(ns + "OrgId").FirstOrDefault()?.Value ?? "";
            string creDtTm = xml.Descendants(ns + "CreDtTm").FirstOrDefault()?.Value ?? "";

            var record = new CamtRawRecord
            {
                camtr_uptddt = DateTime.Now,
                camtr_uptdby = "SYSTEM",
                camtr_rsrcurl = fileName,
                camtr_jobtyp = "AUTO",

                camtr_grphdr_msgid = msgId,
                camtr_grphdr_msgrcpt_nm = msgRcptName,
                camtr_grphdr_msgrcpt_orgid = msgRcptOrgId,
                camtr_grphdr_credttm = DateTime.TryParse(creDtTm, out var t1) ? t1 : DateTime.Now,

                camtr_crtddt = DateTime.Now,
                camtr_crtdby = "SYSTEM",

                // Always NULL
                camtr_bktocstmrstmt = null
            };

            bool ok = await InsertAsync(record);

            Console.WriteLine(ok
                ? $"✅ Inserted RAW XML record ({fileName}) with NULL XML column"
                : $"❌ Failed to Insert ({fileName})");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ XML ERROR: {ex.Message}");
        }
    }

    private static async Task<bool> InsertAsync(CamtRawRecord r)
    {
        const string sql = @"
            INSERT INTO CamtRaw (
                camtr_uptddt, camtr_uptdby, camtr_rsrcurl, camtr_jobtyp,
                camtr_grphdr_msgrcpt_orgid, camtr_grphdr_msgrcpt_nm,
                camtr_grphdr_msgid, camtr_grphdr_credttm,
                camtr_crtddt, camtr_crtdby, camtr_bktocstmrstmt
            )
            VALUES (
                @uptddt, @uptdby, @url, @job,
                @orgid, @nm, @msgid, @cdt,
                @crtddt, @crtdby, @stmt
            );
        ";

        try
        {
            await using var conn = new NpgsqlConnection(ConnectionString);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand(sql, conn);

            cmd.Parameters.AddWithValue("uptddt", r.camtr_uptddt);
            cmd.Parameters.AddWithValue("uptdby", r.camtr_uptdby ?? "");
            cmd.Parameters.AddWithValue("url", r.camtr_rsrcurl ?? "");
            cmd.Parameters.AddWithValue("job", r.camtr_jobtyp ?? "");

            cmd.Parameters.AddWithValue("orgid", r.camtr_grphdr_msgrcpt_orgid ?? "");
            cmd.Parameters.AddWithValue("nm", r.camtr_grphdr_msgrcpt_nm ?? "");
            cmd.Parameters.AddWithValue("msgid", r.camtr_grphdr_msgid ?? "");
            cmd.Parameters.AddWithValue("cdt", r.camtr_grphdr_credttm);

            cmd.Parameters.AddWithValue("crtddt", r.camtr_crtddt);
            cmd.Parameters.AddWithValue("crtdby", r.camtr_crtdby ?? "");

            // Always store NULL in XML column
            var xmlParam = new NpgsqlParameter("stmt", NpgsqlTypes.NpgsqlDbType.Xml)
            {
                Value = DBNull.Value
            };
            cmd.Parameters.Add(xmlParam);

            await cmd.ExecuteNonQueryAsync();
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ DB ERROR: {ex.Message}");
            return false;
        }
    }
}

internal class CamtRawRecord
{
    public DateTime camtr_uptddt { get; set; }
    public string? camtr_uptdby { get; set; }
    public string? camtr_rsrcurl { get; set; }
    public string? camtr_jobtyp { get; set; }

    public string? camtr_grphdr_msgrcpt_orgid { get; set; }
    public string? camtr_grphdr_msgrcpt_nm { get; set; }
    public string? camtr_grphdr_msgid { get; set; }
    public DateTime camtr_grphdr_credttm { get; set; }

    public DateTime camtr_crtddt { get; set; }
    public string? camtr_crtdby { get; set; }

    public string? camtr_bktocstmrstmt { get; set; }   // Always NULL
}

