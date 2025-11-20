using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Linq;
using Npgsql;
 
// namespace CamtFileWatcher
// {
//     internal
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
 
            Console.WriteLine("✅ Initial scan complete. Watching for new files...");
            Console.ReadLine();
        }
 
        private static async Task ProcessXmlFileAsync(string filePath)
        {
            string fileName = Path.GetFileName(filePath);
            Console.WriteLine($"🔍 Processing XML: {fileName}");
 
            try
            {
                var xml = XDocument.Load(filePath);
                XNamespace ns = xml.Root?.GetDefaultNamespace() ?? XNamespace.None;
 
                var stmtList = xml.Descendants(ns + "Stmt").ToList();
                if (stmtList.Count == 0)
                {
                    Console.WriteLine("⚠️ No <Stmt> found.");
                    return;
                }
 
                foreach (var stmt in stmtList)
                {
                    // CAMT fields
                    string grpHdrId = xml.Descendants(ns + "MsgId").FirstOrDefault()?.Value ?? "";
                    string stmtId = stmt.Element(ns + "Id")?.Value ?? "";
                    int.TryParse(stmt.Element(ns + "ElctrncSeqNb")?.Value, out int seqNo);
                    string acctName = stmt.Descendants(ns + "Acct").Elements(ns + "Nm").FirstOrDefault()?.Value ?? "";
                    string acctNumber = stmt.Descendants(ns + "Acct").Descendants(ns + "Id").FirstOrDefault()?.Value ?? "";
                    string bic = stmt.Descendants(ns + "BIC").FirstOrDefault()?.Value ?? "";
                    string creDtTm = xml.Descendants(ns + "CreDtTm").FirstOrDefault()?.Value ?? DateTime.Now.ToString("s");
 
                    // CAMTR fields
                    string trGrpHdrId = xml.Descendants(ns + "MsgId").FirstOrDefault()?.Value ?? "";
                    string trCreDtTm = xml.Descendants(ns + "CreDtTm").FirstOrDefault()?.Value ?? "";
                    string trMsgRcptNm = xml.Descendants(ns + "Nm").FirstOrDefault()?.Value ?? "";
                    string trMsgRcptOrgId = xml.Descendants(ns + "OrgId").FirstOrDefault()?.Value ?? "";
 
                    var entries = stmt.Descendants(ns + "Ntry").ToList();
 
                    foreach (var entry in entries)
                    {
                        var model = ExtractEntryData(
                            entry, ns,
                            grpHdrId, stmtId, seqNo,
                            acctName, acctNumber, bic,
                            creDtTm,
                            trGrpHdrId, trCreDtTm, trMsgRcptNm, trMsgRcptOrgId
                        );
 
                        bool inserted = await InsertAsync(model);
 
                        Console.WriteLine(inserted
                            ? $"✅ Inserted {model.CAMT_NtryRef}"
                            : $"❌ Failed {model.CAMT_NtryRef}"
                        );
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ ERROR: {ex.Message}");
            }
        }
 
        private static CamtNewRecord ExtractEntryData(
            XElement entry, XNamespace ns,
            string grpHdrId, string stmtId, int seqNo,
            string acctName, string acctNo, string bic, string creDtTm,
            string trGrpHdrId, string trCreDtTm, string trMsgRcptNm, string trMsgRcptOrgId
        )
        {
            return new CamtNewRecord
            {
                CAMT_GrpHdr_MsgId = grpHdrId,
                CAMT_StmtId = stmtId,
                CAMT_ElctrncSeqNb = seqNo,
                CAMT_BnkAcctHldrNm = acctName,
                CAMT_BnkAcctNo = acctNo,
                CAMT_BnkId = bic,
                CAMT_NtryRef = entry.Element(ns + "NtryRef")?.Value ?? "",
                CAMT_ValDt =
                    entry.Descendants(ns + "ValDt").Elements(ns + "DtTm").FirstOrDefault()?.Value ??
                    entry.Descendants(ns + "ValDt").Elements(ns + "Dt").FirstOrDefault()?.Value,
 
                CAMT_Ntry_Cd = entry.Descendants(ns + "BkTxCd").Descendants(ns + "Cd").FirstOrDefault()?.Value ?? "",
                CAMT_Ntry_FmlyCd = entry.Descendants(ns + "Fmly").Descendants(ns + "Cd").FirstOrDefault()?.Value ?? "",
                CAMT_Ntry_SubFmlyCd = entry.Descendants(ns + "SubFmlyCd").FirstOrDefault()?.Value ?? "",
                CAMT_Ntry_Prtry_Cd = entry.Descendants(ns + "Prtry").Descendants(ns + "Cd").FirstOrDefault()?.Value ?? "",
 
                CAMT_NtryDtls_AcctSvcrRe = entry.Descendants(ns + "AcctSvcrRef").FirstOrDefault()?.Value ?? "",
                CAMT_NtryDtls_PmtInfId = entry.Descendants(ns + "PmtInfId").FirstOrDefault()?.Value ?? "",
                CAMT_NtryDtls_EndToEndId = entry.Descendants(ns + "EndToEndId").FirstOrDefault()?.Value ?? "",
                CAMT_NtryDtls_TxId = entry.Descendants(ns + "TxId").FirstOrDefault()?.Value ?? "",
                CAMT_NtryDtls_RmtInf_Ustrd = entry.Descendants(ns + "Ustrd").FirstOrDefault()?.Value ?? "",
 
                camt_stmt_ntry_amt_value = entry.Element(ns + "Amt")?.Value ?? "",
                camt_stmt_ntry_amt_ccy = entry.Element(ns + "Amt")?.Attribute("Ccy")?.Value ?? "",
 
                CAMT_BnkStmt_Typ = "CAMT053",
                CAMT_JobTyp = "AUTO",
                CAMT_CrtdDt = DateTime.Now,
 
                // CAMTR fields
                CAMTR_GrpHdr_MsgId = trGrpHdrId,
                CAMTR_GrpHdr_CreDtTm = trCreDtTm,
                CAMTR_GrpHdr_MsgRcpt_Nm = trMsgRcptNm,
                CAMTR_GrpHdr_MsgRcpt_OrgId = trMsgRcptOrgId,
                CAMTR_BkToCstmrStmt = stmtId,
                CAMTR_RsrcUrl = "",
                CAMTR_JobTyp = "AUTO",
                CAMTR_CrtdDt = DateTime.Now
            };
        }
 
        private static async Task<bool> InsertAsync(CamtNewRecord r)
        {
            const string sql = @"
                INSERT INTO Camt53 (
                    CAMT_GrpHdr_MsgId, CAMT_StmtId, CAMT_ElctrncSeqNb,
                    CAMT_BnkAcctHldrNm, CAMT_BnkAcctNo, CAMT_BnkId,
                    CAMT_NtryRef, CAMT_ValDt, CAMT_Ntry_Cd, CAMT_Ntry_FmlyCd,
                    CAMT_Ntry_SubFmlyCd, CAMT_Ntry_Prtry_Cd, CAMT_NtryDtls_AcctSvcrRe,
                    CAMT_NtryDtls_PmtInfId, CAMT_NtryDtls_EndToEndId, CAMT_NtryDtls_TxId,
                    camt_stmt_ntry_amt_value, camt_stmt_ntry_amt_ccy, CAMT_NtryDtls_RmtInf_Ustrd,
                    CAMT_JobTyp, CAMT_BnkStmt_Typ, CAMT_CrtdDt, CAMT_CrtdBy,
 
                    CAMTR_GrpHdr_MsgId, CAMTR_GrpHdr_CreDtTm,
                    CAMTR_GrpHdr_MsgRcpt_Nm, CAMTR_GrpHdr_MsgRcpt_OrgId,
                    CAMTR_BkToCstmrStmt, CAMTR_RsrcUrl, CAMTR_JobTyp,
                    CAMTR_CrtdDt, CAMT_Status
                )
                VALUES (
                    @g, @sid, @seq, @ah, @acn, @bid,
                    @nr, @vd, @cd, @fcd, @scd, @pcd, @asr,
                    @pid, @eid, @tx, @amtv, @amtc, @rm,
                    @job, @bst, @cdt, 'SYSTEM',
 
                    @tg, @tcdt, @trn, @torg,
                    @tbk, @url, @tjob, @tcd, 'ACTIVE'
                );
            ";
 
            try
            {
                await using var conn = new NpgsqlConnection(ConnectionString);
                await conn.OpenAsync();
 
                await using var cmd = new NpgsqlCommand(sql, conn);
 
                cmd.Parameters.AddWithValue("g", r.CAMT_GrpHdr_MsgId ?? "");
                cmd.Parameters.AddWithValue("sid", r.CAMT_StmtId ?? "");
                cmd.Parameters.AddWithValue("seq", r.CAMT_ElctrncSeqNb);
                cmd.Parameters.AddWithValue("ah", r.CAMT_BnkAcctHldrNm ?? "");
                cmd.Parameters.AddWithValue("acn", r.CAMT_BnkAcctNo ?? "");
                cmd.Parameters.AddWithValue("bid", r.CAMT_BnkId ?? "");
                cmd.Parameters.AddWithValue("nr", r.CAMT_NtryRef ?? "");
                cmd.Parameters.AddWithValue("vd", DateTime.TryParse(r.CAMT_ValDt, out var vd) ? vd : (object)DBNull.Value);
                cmd.Parameters.AddWithValue("cd", r.CAMT_Ntry_Cd ?? "");
                cmd.Parameters.AddWithValue("fcd", r.CAMT_Ntry_FmlyCd ?? "");
                cmd.Parameters.AddWithValue("scd", r.CAMT_Ntry_SubFmlyCd ?? "");
                cmd.Parameters.AddWithValue("pcd", r.CAMT_Ntry_Prtry_Cd ?? "");
                cmd.Parameters.AddWithValue("asr", r.CAMT_NtryDtls_AcctSvcrRe ?? "");
                cmd.Parameters.AddWithValue("pid", r.CAMT_NtryDtls_PmtInfId ?? "");
                cmd.Parameters.AddWithValue("eid", r.CAMT_NtryDtls_EndToEndId ?? "");
                cmd.Parameters.AddWithValue("tx", r.CAMT_NtryDtls_TxId ?? "");
                cmd.Parameters.AddWithValue("amtv", r.camt_stmt_ntry_amt_value ?? "");
                cmd.Parameters.AddWithValue("amtc", r.camt_stmt_ntry_amt_ccy ?? "");
                cmd.Parameters.AddWithValue("rm", r.CAMT_NtryDtls_RmtInf_Ustrd ?? "");
                cmd.Parameters.AddWithValue("job", r.CAMT_JobTyp ?? "");
                cmd.Parameters.AddWithValue("bst", r.CAMT_BnkStmt_Typ ?? "");
                cmd.Parameters.AddWithValue("cdt", r.CAMT_CrtdDt);
 
                // CAMTR
                cmd.Parameters.AddWithValue("tg", r.CAMTR_GrpHdr_MsgId ?? "");
                cmd.Parameters.AddWithValue("tcdt", r.CAMTR_GrpHdr_CreDtTm ?? "");
                cmd.Parameters.AddWithValue("trn", r.CAMTR_GrpHdr_MsgRcpt_Nm ?? "");
                cmd.Parameters.AddWithValue("torg", r.CAMTR_GrpHdr_MsgRcpt_OrgId ?? "");
                cmd.Parameters.AddWithValue("tbk", r.CAMTR_BkToCstmrStmt ?? "");
                cmd.Parameters.AddWithValue("url", r.CAMTR_RsrcUrl ?? "");
                cmd.Parameters.AddWithValue("tjob", r.CAMTR_JobTyp ?? "");
                cmd.Parameters.AddWithValue("tcd", r.CAMTR_CrtdDt);
 
                await cmd.ExecuteNonQueryAsync();
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine("❌ Insert Error: " + ex.Message);
                return false;
            }
        }
    }
 
    internal class CamtNewRecord
    {
        public string? CAMT_GrpHdr_MsgId { get; set; }
        public string? CAMT_StmtId { get; set; }
        public int CAMT_ElctrncSeqNb { get; set; }
        public string? CAMT_BnkAcctHldrNm { get; set; }
        public string? CAMT_BnkAcctNo { get; set; }
        public string? CAMT_BnkId { get; set; }
 
        public string? CAMT_NtryRef { get; set; }
        public string? CAMT_ValDt { get; set; }
 
        public string? CAMT_Ntry_Cd { get; set; }
        public string? CAMT_Ntry_FmlyCd { get; set; }
        public string? CAMT_Ntry_SubFmlyCd { get; set; }
        public string? CAMT_Ntry_Prtry_Cd { get; set; }
 
        public string? CAMT_NtryDtls_AcctSvcrRe { get; set; }
        public string? CAMT_NtryDtls_PmtInfId { get; set; }
        public string? CAMT_NtryDtls_EndToEndId { get; set; }
        public string? CAMT_NtryDtls_TxId { get; set; }
        public string? CAMT_NtryDtls_RmtInf_Ustrd { get; set; }
 
        public string? camt_stmt_ntry_amt_value { get; set; }
        public string? camt_stmt_ntry_amt_ccy { get; set; }
 
        public string? CAMT_JobTyp { get; set; }
        public string? CAMT_BnkStmt_Typ { get; set; }
        public DateTime CAMT_CrtdDt { get; set; }
 
        // CAMTR
        public string? CAMTR_GrpHdr_MsgId { get; set; }
        public string? CAMTR_GrpHdr_CreDtTm { get; set; }
        public string? CAMTR_GrpHdr_MsgRcpt_Nm { get; set; }
        public string? CAMTR_GrpHdr_MsgRcpt_OrgId { get; set; }
        public string? CAMTR_BkToCstmrStmt { get; set; }
        public string? CAMTR_RsrcUrl { get; set; }
        public string? CAMTR_JobTyp { get; set; }
        public DateTime CAMTR_CrtdDt { get; set; }
    }
