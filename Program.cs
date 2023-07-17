using Newtonsoft.Json.Linq;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ClosedXML.Excel;
using System.Net.Mail;
using System.Web;
using System.Globalization;


namespace PECPayrollReporting
{
    internal class Program
    {
        #region Global Declaration

        public static string ModuleId = string.Empty, InputPath = string.Empty, BaseDirectoryPath = AppDomain.CurrentDomain.BaseDirectory,
            WebServiceURL = string.Empty, ErrorLogPath = string.Empty, ConnectionString = string.Empty, ArchivePath = string.Empty,
            SFTPServer = string.Empty, SFTPUserName = string.Empty, SFTPPassword = string.Empty, SFTPRemoteDirectory = string.Empty,
            ActionLogPath = string.Empty, OutputPath = string.Empty, ZipPath = string.Empty, ProcessedArchivePath = string.Empty, PayrollDocPath = string.Empty,
            MailSubject = string.Empty, FromMailId = string.Empty, SenderName = string.Empty, CCMails = string.Empty, BCCMails = string.Empty,
            LogPath = string.Empty, PageRedirectionMVCURL = string.Empty, ExceptionSubject = string.Empty,
            IP = string.Empty, Username = string.Empty, Password = string.Empty, remoteDirectory = string.Empty, PayrollDataPath = string.Empty;

        #endregion

        static async Task Main(string[] args)
        {
            InitializeConfig();

            /*Download and insert Metafile*/
            //await DownloadOBDocsDataAsync();
            //var OBDocsData = await OBDocsAsync();
            //await OBDocsSqlInsertAsync(OBDocsData);

            ///*Download and insert Payroll data*/
            //await DownloadPayrollDataAsync();
            //await PayrollDataAsync();

            //var todayDate = DateTime.Now;
            //var day = todayDate.Day.ToString();
            //var lastDay = DateTime.DaysInMonth(todayDate.Year, todayDate.Month).ToString();

            //if (day == "10" || day == "12" || day == "20" || day == lastDay)
            //{
            /*Download Document*/
            //await DownloadPayrollDocs();

            /*Upload to SFTP*/
            //await UploadFilesToServerAsync();

            /*Nudges to Payroll*/
            GetDetailsandSendReminders();
            //}

            /*Archive Metadata*/
            //await OBDocsSqlArchiveAsync();

        }
        public static void InitializeConfig()
        {

            SFTPServer = ConfigurationManager.AppSettings["SFTPServer"].ToString();
            SFTPPassword = ConfigurationManager.AppSettings["SFTPPassword"].ToString();
            SFTPUserName = ConfigurationManager.AppSettings["SFTPUserName"].ToString();
            SFTPRemoteDirectory = ConfigurationManager.AppSettings["SFTPRemoteDirectory"].ToString();

            IP = ConfigurationManager.AppSettings["IP"].ToString();
            Username = ConfigurationManager.AppSettings["Username"].ToString();
            Password = ConfigurationManager.AppSettings["Password"].ToString();
            remoteDirectory = ConfigurationManager.AppSettings["remoteDirectory"].ToString();
            PayrollDataPath = BaseDirectoryPath + "Input/Payroll/";

            ConnectionString = ConfigurationManager.ConnectionStrings["Godrejite"].ConnectionString;
            ModuleId = ConfigurationManager.AppSettings["ModuleId"].ToString();
            InputPath = BaseDirectoryPath + "Input/";
            WebServiceURL = ConfigurationManager.AppSettings["WebServiceURL"].ToString();
            ErrorLogPath = BaseDirectoryPath + "Logs/Error";
            ArchivePath = BaseDirectoryPath + "Input/Archive";
            ActionLogPath = BaseDirectoryPath + "Logs/Action";
            ProcessedArchivePath = BaseDirectoryPath + "Output/Archive/" + DateTime.Today.ToString("ddMMMyyyy");
            OutputPath = BaseDirectoryPath + "Output/";
            ZipPath = BaseDirectoryPath + "Output/ZipPath/" + DateTime.Today.ToString("ddMMMyyyy");
            PayrollDocPath = BaseDirectoryPath + "PayrollDocPath/" + DateTime.Today.ToString("ddMMMyyyy");

            MailSubject = ConfigurationManager.AppSettings["MailSubject"].ToString();
            FromMailId = ConfigurationManager.AppSettings["FromMailId"].ToString();
            SenderName = ConfigurationManager.AppSettings["SenderName"].ToString();
            CCMails = ConfigurationManager.AppSettings["CCMails"].ToString();
            BCCMails = ConfigurationManager.AppSettings["BCCMails"].ToString();
            PageRedirectionMVCURL = ConfigurationManager.AppSettings["PageRedirectionMVCURL"].ToString();
            LogPath = BaseDirectoryPath + "Logs";
            ExceptionSubject = ConfigurationManager.AppSettings["ExceptionSubject"].ToString();

            if (!Directory.Exists(OutputPath))
                Directory.CreateDirectory(OutputPath);

            if (!Directory.Exists(ErrorLogPath))
                Directory.CreateDirectory(ErrorLogPath);

            if (!Directory.Exists(ArchivePath))
                Directory.CreateDirectory(ArchivePath);

            if (!Directory.Exists(ActionLogPath))
                Directory.CreateDirectory(ActionLogPath);

            if (!Directory.Exists(ProcessedArchivePath))
                Directory.CreateDirectory(ProcessedArchivePath);

            if (!Directory.Exists(LogPath))
                Directory.CreateDirectory(LogPath);

            ErrorLogPath = ErrorLogPath + "/Error.txt";

            ActionLogPath = ActionLogPath + "ActionLog.txt";

            if (!File.Exists(ErrorLogPath))
            {
                using (FileStream fs = File.Create(ErrorLogPath))
                {
                    // Add some text to file    
                    Byte[] title = new UTF8Encoding(true).GetBytes("PEC Payroll Document Automation Error Logs" + Environment.NewLine);
                    fs.Write(title, 0, title.Length);
                    byte[] author = new UTF8Encoding(true).GetBytes("---------------------------------------------------------" + Environment.NewLine);
                    fs.Write(author, 0, author.Length);
                }
            }

            if (!File.Exists(ActionLogPath))
            {
                using (FileStream fs = File.Create(ActionLogPath))
                {
                    // Add some text to file    
                    Byte[] title = new UTF8Encoding(true).GetBytes("PEC Payroll Document Automation Action Logs" + Environment.NewLine);
                    fs.Write(title, 0, title.Length);
                    byte[] author = new UTF8Encoding(true).GetBytes("---------------------------------------------------------" + Environment.NewLine);
                    fs.Write(author, 0, author.Length);
                }
            }

        }

        #region Download and insert Metafile       
        static async Task DownloadOBDocsDataAsync()
        {
            try
            {
                using (var sftp = new SftpClient(SFTPServer, SFTPUserName, SFTPPassword))
                {
                    Console.WriteLine("Connecting to " + SFTPServer + " as " + SFTPUserName);
                    sftp.Connect();
                    Console.WriteLine("Connected!");
                    var files = sftp.ListDirectory(SFTPRemoteDirectory);

                    var OBDocsFile = files.OrderByDescending(_ => _.LastWriteTime).FirstOrDefault(_ => _.Name.ToLower().Contains("documentmetafile_"));

                    string OBDocsFileName = OBDocsFile.Name;

                    using (StreamWriter sw = File.AppendText(ActionLogPath))
                    {
                        sw.WriteLine("Downloading File: " + OBDocsFileName + System.DateTime.Now.ToString());
                    }

                    using (Stream OBDocsFile1 = File.OpenWrite(InputPath + OBDocsFileName))
                    {
                        Console.WriteLine("Downloading file " + OBDocsFileName);
                        sftp.DownloadFile(SFTPRemoteDirectory + OBDocsFileName, OBDocsFile1);
                    }

                    Console.WriteLine("Download Completed !!");
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("DownloadInput: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Data Sync Excecuted Failed", "DownloadOBDocsDataAsync", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        public static async Task<DataTable> OBDocsAsync()
        {
            var importedData = new DataTable();
            int count = Directory.GetFiles(InputPath, "*.csv").Length;
            try
            {
                if (count > 0)
                {
                    string filepath = Directory.GetFiles(InputPath, "*.csv")[0].ToString();
                    using (StreamReader sr = new StreamReader(filepath))
                    {
                        string header = sr.ReadLine().Replace("\"", "");
                        if (!string.IsNullOrEmpty(header))
                        {
                            string[] headerColumns = header.Split('|');
                            foreach (string headerColumn in headerColumns)
                            {
                                importedData.Columns.Add(headerColumn);
                            }
                            importedData.Columns.Add("CreatedBy");
                            Regex csvParser = new Regex(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");

                            while (!sr.EndOfStream)
                            {

                                string line = sr.ReadLine();
                                if (string.IsNullOrEmpty(line)) continue;
                                string[] fields = csvParser.Split(line);

                                DataRow importedRow = importedData.NewRow();

                                string[] rowsData = fields[0].Split('|');

                                int i = 0;

                                for (i = 0; i < rowsData.Count(); i++)
                                {
                                    rowsData[i] = rowsData[i].TrimStart(' ', '"');
                                    rowsData[i] = rowsData[i].TrimEnd('"');
                                    importedRow[i] = rowsData[i];
                                }

                                importedRow["CreatedBy"] = "999999";

                                importedData.Rows.Add(importedRow);
                            }

                        }
                    }
                    File.Delete(filepath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("the file could not be read:");
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("the file could not be read(PSData): " + ex.Message + System.DateTime.Now.ToString());
                }

                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Data Sync Excecuted Failed", "OBDocsAsync", "Failed", ex.Message.ToString(), "", "999999");
            }

            return importedData;
        }
        static async Task OBDocsSqlInsertAsync(DataTable dt)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(ConnectionString))
                {
                    using (SqlBulkCopy objbulk = new SqlBulkCopy(con))
                    {
                        objbulk.DestinationTableName = "tbl_PS_OBDocs";

                        objbulk.ColumnMappings.Add("EmployeeCode", "EmployeeCode");
                        objbulk.ColumnMappings.Add("DocumentName", "DocumentName");
                        objbulk.ColumnMappings.Add("PickList", "PickList");
                        objbulk.ColumnMappings.Add("Docpath", "Docpath");
                        objbulk.ColumnMappings.Add("FileName", "FileName");
                        objbulk.ColumnMappings.Add("CreatedBy", "CreatedBy");

                        con.Open();
                        objbulk.WriteToServer(dt);
                        con.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("SqlInsert: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Data Sync Excecuted Failed", "OBDocsSqlInsertAsync", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        #endregion

        #region Download and insert Payroll data
        public static async Task DownloadPayrollDataAsync()
        {
            try
            {
                using (var sftp = new SftpClient(IP, Username, Password))
                {
                    Console.WriteLine("Connecting to " + IP + " as " + Username);
                    sftp.Connect();
                    Console.WriteLine("Connected!");
                    var files = sftp.ListDirectory(remoteDirectory);

                    foreach (var file in files)
                    {
                        if ((!file.Name.StartsWith(".")))
                        {
                            var PayrollDataFile = "Payroll_Input_" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy") + ".csv";

                            if (file.Name.ToString().Contains(PayrollDataFile))
                            {
                                using (StreamWriter sw = File.AppendText(ActionLogPath))
                                {
                                    sw.WriteLine("Downloading File: " + file.Name + System.DateTime.Now.ToString());
                                }

                                using (Stream payrollDataFile1 = File.OpenWrite(PayrollDataPath + file.Name))
                                {
                                    Console.WriteLine("Downloading file " + file.Name);
                                    sftp.DownloadFile(remoteDirectory + file.Name, payrollDataFile1);
                                }

                                Console.WriteLine("Download Completed !!");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("DownloadInput: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Data Sync Excecuted Failed", "DownloadPayrollDataAsync", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        public static async Task PayrollDataAsync()
        {
            try
            {
                DataTable data = await PayrollDataInput();

                if (data.Rows.Count > 0)
                {
                    foreach (DataRow dr in data.Rows)
                    {
                        var EMP_NO = dr["EMP NO"].ToString();
                        var EMP_NAME = dr["EMP NAME"].ToString();
                        var PAY_GROUP = dr["PAY GROUP"].ToString();
                        //var DOB = DateTime.Parse(dr["DOB (DD/MM/YYYY)"].ToString());
                        var DOB = DateTime.ParseExact(dr["DOB (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture);
                        var GENDER = dr["GENDER"].ToString();
                        var EMAIL_ID = dr["EMAIL ID"].ToString();
                        var sUSERNAME = dr["USERNAME"].ToString();
                        var FATHER_NAME = dr["FATHER NAME"].ToString();
                        var DESIGNATION = dr["DESIGNATION"].ToString();
                        var SALARY_GRADE = dr["SALARY GRADE"].ToString();
                        var DEPARTMENT = dr["DEPARTMENT"].ToString();
                        var LOCATION = dr["LOCATION(City Name)"].ToString();
                        var COST_CENTRE = dr["COST CENTRE"].ToString();
                        //var DOJ = DateTime.Parse(dr["DATE OF JOINING (DD/MM/YYYY)"].ToString());
                        var DOJ = DateTime.ParseExact(dr["DATE OF JOINING (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture);
                        var PAN_NO = dr["PAN NO"].ToString();
                        var Aadhar_card = dr["Aadhar card"].ToString();
                        var Marital_Status = dr["Marital Status"].ToString();
                        var PAYMENT_MODE = dr["PAYMENT MODE"].ToString();
                        var Bank_Name = dr["Bank Name-Bank Details"].ToString();
                        var Branch_Name = dr["Branch Name-Bank Details"].ToString();
                        var Bank_Account_Number = dr["Bank Account Number-Bank Details"].ToString();
                        var IFSC_Code = dr["IFSC Code-Bank Details"].ToString();
                        var Education_Qualifications = dr["Education Qualifications_employee-Education Qual"].ToString();
                        var Basic = dr["Basic(per month)"].ToString();
                        var PF = dr["PF(per month)"].ToString();
                        var Gratuity = dr["Gratuity(per month)"].ToString();
                        var Employee_Retirals = dr["Employee Retirals(per month)"].ToString();
                        var Education_Allowance = dr["Education Allowance(per month)"].ToString();
                        var Sodexho = dr["Sodexho(per month)"].ToString();
                        var HRA = dr["HRA(per month)"].ToString();
                        var LTA = dr["LTA(per month)"].ToString();
                        var Conveyance = dr["Conveyance(per month)"].ToString();
                        var Telephone_Reimbursement = dr["Telephone Reimbursement(per month)"].ToString();
                        var Driver_Salary_Allowance = dr["Driver Salary Allowance(per annum)"].ToString();
                        var Supplementary_Allowance = dr["Supplementary Allowance(per month)"].ToString();
                        var Flexi = dr["Flexi(per month)"].ToString();
                        var Ex_Gratia = dr["Ex Gratia /statutory Bonus(Per month)"].ToString();
                        var Total_Fixed_Component = dr["Total Fixed Component"].ToString();
                        var PLVR_I = dr["PLVR I / PBFT I(Per Annum)"].ToString();
                        var PLVR_C = dr["PLVR C / PBFT C I (Per Annum)"].ToString();
                        var Total_PBFT = dr["Total PBFT/PLVR"].ToString();
                        var TOTAL_CTC = dr["TOTAL CTC"].ToString();
                        var Joining_Bonus = dr["Joining Bonus(per month)"].ToString();
                        var Clause_Compensation = dr["Clause-Compensation Information"].ToString();
                        var SBU = dr["Sub Business Unit"].ToString();
                        var Region = dr["Region"].ToString();
                        var Candidate_Address = dr["Candidate Address"].ToString();
                        var UAN = dr["UAN No"].ToString();

                        using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["Godrejite"].ConnectionString))
                        {
                            SqlCommand cmd = new SqlCommand("USP_InsertPayrollData", conn);
                            cmd.Parameters.AddWithValue("@EMP_NO", EMP_NO == null ? "" : EMP_NO);
                            cmd.Parameters.AddWithValue("@EMP_NAME", EMP_NAME == null ? "" : EMP_NAME);
                            cmd.Parameters.AddWithValue("@PAY_GROUP", PAY_GROUP == null ? "" : PAY_GROUP);
                            cmd.Parameters.AddWithValue("@DOB", DOB);
                            cmd.Parameters.AddWithValue("@GENDER", GENDER == null ? "" : GENDER);
                            cmd.Parameters.AddWithValue("@EMAIL_ID", EMAIL_ID == null ? "" : EMAIL_ID);
                            cmd.Parameters.AddWithValue("@USERNAME", sUSERNAME == null ? "" : sUSERNAME);
                            cmd.Parameters.AddWithValue("@FATHER_NAME", FATHER_NAME == null ? "" : FATHER_NAME);
                            cmd.Parameters.AddWithValue("@DESIGNATION", DESIGNATION == null ? "" : DESIGNATION);
                            cmd.Parameters.AddWithValue("@SALARY_GRADE", SALARY_GRADE == null ? "" : SALARY_GRADE);
                            cmd.Parameters.AddWithValue("@DEPARTMENT", DEPARTMENT == null ? "" : DEPARTMENT);
                            cmd.Parameters.AddWithValue("@LOCATION", LOCATION == null ? "" : LOCATION);
                            cmd.Parameters.AddWithValue("@COST_CENTRE", COST_CENTRE == null ? "" : COST_CENTRE);
                            cmd.Parameters.AddWithValue("@DATE_OF_JOINING", DOJ);
                            cmd.Parameters.AddWithValue("@PAN_NO", PAN_NO == null ? "" : PAN_NO);
                            cmd.Parameters.AddWithValue("@Aadhar_card", Aadhar_card == null ? "" : PAN_NO);
                            cmd.Parameters.AddWithValue("@Marital_Status", Marital_Status == null ? "" : Marital_Status);
                            cmd.Parameters.AddWithValue("@PAYMENT_MODE", PAYMENT_MODE == null ? "" : PAYMENT_MODE);
                            cmd.Parameters.AddWithValue("@Bank_Name", Bank_Name == null ? "" : Bank_Name);
                            cmd.Parameters.AddWithValue("@Branch_Name", Branch_Name == null ? "" : Branch_Name);
                            cmd.Parameters.AddWithValue("@Bank_Account_Number", Bank_Account_Number == null ? "" : Bank_Account_Number);
                            cmd.Parameters.AddWithValue("@IFSC_Code", IFSC_Code == null ? "" : IFSC_Code);
                            cmd.Parameters.AddWithValue("@Education_Qualifications", Education_Qualifications == null ? "" : Education_Qualifications);
                            cmd.Parameters.AddWithValue("@Basic_Monthly", Basic);
                            cmd.Parameters.AddWithValue("@PF_Monthly", PF);
                            cmd.Parameters.AddWithValue("@Gratuity_Monthly", Gratuity);
                            cmd.Parameters.AddWithValue("@Employee_Retirals_Monthly", Employee_Retirals);
                            cmd.Parameters.AddWithValue("@Education_Allowance_Monthly", Education_Allowance);
                            cmd.Parameters.AddWithValue("@Sodexho_Monthly", Sodexho);
                            cmd.Parameters.AddWithValue("@HRA_Monthly", HRA);
                            cmd.Parameters.AddWithValue("@LTA_Monthly", LTA);
                            cmd.Parameters.AddWithValue("@Conveyance_Monthly", Conveyance);
                            cmd.Parameters.AddWithValue("@Telephone_Reimbursement_Monthly", Telephone_Reimbursement);
                            cmd.Parameters.AddWithValue("@Driver_Salary_Allowance_Annum", Driver_Salary_Allowance);
                            cmd.Parameters.AddWithValue("@Supplementary_Allowance_Monthly", Supplementary_Allowance);
                            cmd.Parameters.AddWithValue("@Flexi_Monthly", Flexi);
                            cmd.Parameters.AddWithValue("@Ex_Gratia_statutory_Bonus_Monthly", Ex_Gratia);
                            cmd.Parameters.AddWithValue("@Total_Fixed_Component", Total_Fixed_Component);
                            cmd.Parameters.AddWithValue("@PLVR_I_PBFT_I_Annum", PLVR_I);
                            cmd.Parameters.AddWithValue("@PLVR_C_PBFT_C_I_Annum", PLVR_C);
                            cmd.Parameters.AddWithValue("@Total_PBFT_PLVR", Total_PBFT);
                            cmd.Parameters.AddWithValue("@TOTAL_CTC", TOTAL_CTC);
                            cmd.Parameters.AddWithValue("@Joining_Bonus_Monthly", Joining_Bonus);
                            cmd.Parameters.AddWithValue("@Clause_Compensation_Information", Clause_Compensation);
                            cmd.Parameters.AddWithValue("@Sub_Business_Unit", SBU == null ? "" : SBU);
                            cmd.Parameters.AddWithValue("@Region", Region == null ? "" : Region);
                            cmd.Parameters.AddWithValue("@Candidate_Address", Candidate_Address == null ? "" : Candidate_Address);
                            cmd.Parameters.AddWithValue("@UAN", UAN == null ? "" : UAN);

                            cmd.CommandType = CommandType.StoredProcedure;
                            conn.Open();

                            cmd.ExecuteNonQuery();
                            conn.Close();
                            conn.Dispose();
                        }
                    }

                    var result = await SchedulersLogInsertionAsync("Payroll Data Sync Excecuted Successfully", "PayrollDataAsync", "Success", "None", "", "999999");
                }
                else
                {
                    var result = await SchedulersLogInsertionAsync("Payroll Data Sync Excecuted Successfully But No Records Inserted", "PayrollDataAsync", "Success", "None", "", "999999");
                }
            }
            catch (Exception ex)
            {

                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("Error In Method (PayrollDataAsync): " + ex.Message + DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("Payroll Data Sync Excecuted Failed", "PayrollDataSync", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        public static async Task<DataTable> PayrollDataInput()
        {
            var importedData = new DataTable();
            string fileDateFormat = DateTime.Now.AddDays(-1).ToString("ddMMyyyy") + ".csv";
            int count = Directory.GetFiles(PayrollDataPath, "Payroll_Input_" + fileDateFormat).Length;
            try
            {
                DataTable tblcsv = new DataTable();
                if (count > 0)
                {
                    string filepath = Directory.GetFiles(PayrollDataPath, "Payroll_Input_" + fileDateFormat)[0].ToString();
                    using (StreamReader sr = new StreamReader(filepath))
                    {
                        string header = sr.ReadLine().Replace("\"", "");
                        if (!string.IsNullOrEmpty(header))
                        {
                            string[] headerColumns = header.Split(',');
                            foreach (string headerColumn in headerColumns)
                            {
                                importedData.Columns.Add(headerColumn);
                            }

                            Regex csvParser = new Regex(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");

                            while (!sr.EndOfStream)
                            {
                                string line = sr.ReadLine();
                                if (string.IsNullOrEmpty(line)) continue;
                                string[] fields = csvParser.Split(line);

                                DataRow importedRow = importedData.NewRow();

                                for (int i = 0; i < fields.Count(); i++)
                                {
                                    fields[i] = fields[i].TrimStart(' ', '"');
                                    fields[i] = fields[i].TrimEnd('"');
                                    importedRow[i] = fields[i];
                                }
                                importedData.Rows.Add(importedRow);

                            }

                        }
                    }
                    File.Delete(filepath);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("the file could not be read:");

                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("the file could not be read: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("Payroll Data Sync Excecuted Failed", "PayrollDataInput", "Failed", ex.Message.ToString(), "", "999999");
            }

            return importedData;
        }
        #endregion

        #region Download Document
        static async Task DownloadPayrollDocs()
        {
            try
            {
                DataTable dtDocsData = new DataTable();

                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    SqlCommand DocCmd = new SqlCommand("USP_PS_GetPayrollDoc", connection);
                    DocCmd.CommandType = CommandType.StoredProcedure;
                    SqlDataAdapter DocAdp = new SqlDataAdapter(DocCmd);
                    using (DataSet dsDocsData = new DataSet())
                    {
                        DocAdp.Fill(dsDocsData);
                        connection.Close();
                        dtDocsData = dsDocsData.Tables[0];
                    }
                }

                using (var sftp = new SftpClient(SFTPServer, SFTPUserName, SFTPPassword))
                {
                    Console.WriteLine("Connecting to " + SFTPServer + " as " + SFTPUserName);
                    sftp.Connect();
                    Console.WriteLine("Connected!");
                    if (!Directory.Exists(PayrollDocPath))
                        Directory.CreateDirectory(PayrollDocPath);

                    foreach (DataRow dr in dtDocsData.Rows)
                    {
                        var SFTPDocPath = dr[3].ToString().Replace(dr[4].ToString(), "");

                        string DocsFileName = dr[4].ToString();

                        var EmployeeDirectory = PayrollDocPath + "/" + dr[5].ToString() + "/" + dr[0].ToString() + "/";
                        if (!Directory.Exists(EmployeeDirectory))
                            Directory.CreateDirectory(EmployeeDirectory);

                        using (Stream DocsFile1 = File.OpenWrite(EmployeeDirectory + DocsFileName))
                        {
                            Console.WriteLine("Downloading file " + DocsFileName);
                            sftp.DownloadFile(SFTPDocPath + DocsFileName, DocsFile1);
                        }

                    }

                    Console.WriteLine("Download Completed !!");
                }

                if (!Directory.Exists(ZipPath))
                    Directory.CreateDirectory(ZipPath);

                var listComapny = dtDocsData.AsEnumerable().GroupBy(x => x.Field<string>("CompanyCode"))
               .Select(grp => grp.First())
               .ToList();

                foreach (DataRow dr in listComapny)
                {
                    var CompanyDirectory = PayrollDocPath + "/" + dr[5].ToString();
                    System.IO.Compression.ZipFile.CreateFromDirectory(CompanyDirectory, ZipPath + "/" + dr[5].ToString() + ".zip");
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("DownloadInput: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Data Sync Excecuted Failed", "DownloadPayrollDocs", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        #endregion

        #region Upload to SFTP
        static async Task UploadFilesToServerAsync()
        {
            try
            {
                using (var sftp = new SftpClient(SFTPServer, SFTPUserName, SFTPPassword))
                {
                    Console.WriteLine("Connecting to " + SFTPServer + " as " + SFTPUserName);
                    sftp.Connect();
                    Console.WriteLine("Connected!");


                    string[] files = Directory.GetFiles(ZipPath);
                    string fileName = string.Empty, destFile = string.Empty, SFTPDirectory = string.Empty;

                    SFTPDirectory = "/Outbound/PayrollDocument/" + DateTime.Today.ToString("ddMMMyyyy");
                    if (!Directory.Exists(SFTPDirectory))
                        sftp.CreateDirectory(SFTPDirectory);

                    foreach (string s in files)
                    {
                        fileName = Path.GetFileName(s);

                        sftp.ChangeDirectory(SFTPDirectory + "/");

                        using (FileStream fs = new FileStream(s, FileMode.Open))
                        {
                            sftp.BufferSize = 1024;
                            sftp.UploadFile(fs, Path.GetFileName(s));
                        }


                        destFile = Path.Combine(ProcessedArchivePath, fileName);
                        File.Copy(s, destFile, true);
                        File.Delete(s);
                    }
                    sftp.Dispose();
                }

                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Excecuted Successfully", "UploadFilesToServerAsync", "Success", "None", "", "999999");

            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("DownloadInput: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Failed", "UploadFilesToServerAsync", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        #endregion

        #region Nudges to Payroll
        public static void GetDetailsandSendReminders()
        {
            EncryptDecrypt encryptDecrypt = new EncryptDecrypt();
            var objLog = new EmailTemplate();
            try
            {
                using (DataSet ds = new DataSet())
                {
                    using (SqlConnection con = new SqlConnection(ConnectionString))
                    {
                        using (SqlCommand cmd = new SqlCommand("USP_PS_GetOBPayrollDetails", con))
                        {
                            cmd.CommandType = CommandType.StoredProcedure;
                            con.Open();
                            using (SqlDataAdapter adp = new SqlDataAdapter(cmd))
                            {
                                adp.Fill(ds);
                            }
                            con.Close();
                        }
                    }
                    var ListCompany = ds.Tables[0].AsEnumerable().Select(x => x.Field<string>("CompanyCode")).Distinct().ToList();
                    if (ListCompany.Count() > 0)
                    {
                        foreach (var cm in ListCompany)
                        {
                            string CurrentMailSubject = MailSubject, FormLink = string.Empty;
                            var PayrollSpocs = ds.Tables[1].AsEnumerable().Where(x => x.Field<string>("CompanyCode") == cm.ToString());
                            var fullData = ds.Tables[0].AsEnumerable().Where(x => x.Field<string>("CompanyCode") == cm.ToString()).CopyToDataTable();
                            var mailData = fullData.AsEnumerable().Where(x => x.Field<string>("CTC (Per Annum)") != "0" && x.Field<string>("CTC (Per Annum)") != "");
                            var exceptionData = fullData.AsEnumerable().Where(x => x.Field<string>("CTC (Per Annum)") == "0" || x.Field<string>("CTC (Per Annum)") == "");
                            DataTable data = new DataTable();
                            DataTable exception = new DataTable();
                            DataTable PayrollSpoc = new DataTable();
                            data = mailData.Count() > 0 ? mailData.CopyToDataTable() : data;
                            exception = exceptionData.Count() > 0 ? exceptionData.CopyToDataTable() : exception;
                            PayrollSpoc = PayrollSpocs.Count() > 0 ? PayrollSpocs.CopyToDataTable() : PayrollSpoc;
                            if (PayrollSpocs.Count() > 0)
                            {
                                if (data.Rows.Count > 0)
                                {
                                    foreach (DataRow dr in PayrollSpoc.Rows)
                                    {
                                        var RowsHtml = string.Empty;
                                        var FromDate = DateTime.ParseExact(data.Rows[0]["Date of Joining GILAC (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture).ToString("dd MMM yyyy");
                                        var ToDate = DateTime.ParseExact(data.Rows[data.Rows.Count - 1]["Date of Joining GILAC (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture).ToString("dd MMM yyyy");
                                        CurrentMailSubject = string.Format(CurrentMailSubject, FromDate, ToDate);
                                        var Company = dr["CompanyCode"].ToString();
                                        var Folder = DateTime.Today.ToString("ddMMMyyyy");
                                        //FormLink = PageRedirectionMVCURL + "?a=" + HttpUtility.UrlEncode(encryptDecrypt.Encrypt(dr["EmpCode"].ToString())) +
                                        //    "&f=" + HttpUtility.UrlEncode(encryptDecrypt.Encrypt(DateTime.Today.ToString("ddMMMyyyy"))) +
                                        //    "&c=" + HttpUtility.UrlEncode(encryptDecrypt.Encrypt(dr["CompanyCode"].ToString())) +
                                        //    "&m=" + HttpUtility.UrlEncode(encryptDecrypt.Encrypt(ModuleId)) + "";
                                        FormLink = "https://mobi.godrejite.com/Mobility-Launch/PayrollDocs/" + Folder + "/" + Company + ".zip";
                                        RowsHtml += "<tr style='background-color: #fff;height:50px;font-family:Calibri;font-size:13pt;'>";
                                        RowsHtml += "<td align='center'><p style='font-family:Calibri;font-size:13pt;'>Payroll Reporting Documents</p></td>";
                                        RowsHtml += "<td align='center'><a href='" + FormLink + "' target='_blank' title='Click to Action'><img src='https://mobi.godrejite.com/Mobility-Launch/images/ExitInterview/FormLink.png' width='120' alt='Godrej' style='width: 120px; display:block;'></a></td>";
                                        RowsHtml += "</tr>";

                                        string Contenthtml = File.ReadAllText(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase).Replace("file:\\", "") + "\\Email HTML\\PayrollReminder.HTML");
                                        Contenthtml = string.Format(Contenthtml, CurrentMailSubject, "<style type='text/css'> area:focus{ border: none; outline-style: none; -moz-outline-style:none; } map area { outline: none; }img { display: block; } a { cursor: pointer; }</style>", FromDate, ToDate, RowsHtml);

                                        if (CCMails.Length > 0)
                                        {
                                            if (dr["CCMailIds"].ToString().Length > 0)
                                                CCMails = CCMails + ',' + dr["CCMailIds"].ToString();
                                        }
                                        else
                                        {
                                            if (dr["CCMailIds"].ToString().Length > 0)
                                                CCMails = dr["CCMailIds"].ToString();
                                        }

                                        objLog.MailSubject = CurrentMailSubject + " | " + dr["CompanyCode"].ToString();
                                        objLog.FromMail = FromMailId;
                                        objLog.ModuleId = ModuleId;
                                        objLog.FromMailName = SenderName;
                                        //objLog.ToMail = dr["EmailID"].ToString();
                                        objLog.ToMail = "amolkvjob@gmail.com";
                                        objLog.UserId = "";
                                        objLog.CcMail = CCMails;
                                        objLog.BccMail = BCCMails;
                                        objLog.HasError = "N";
                                        objLog.ErrorMsg = "";
                                        objLog.MailSent = "N";
                                        objLog.NotificationLogId = "0";
                                        string NotificationLogId = MailSentMailLog(objLog);
                                        objLog.NotificationLogId = NotificationLogId;
                                        Contenthtml += "<span style='mso-element:field-begin;'></span><img src='" + WebServiceURL + "InsertIJPEREFMailOpenLogs/" + objLog.UserId + "/" + NotificationLogId + "/" + objLog.ModuleId + "' alt='' class='outlookhide hideoutlookweb' style='display:none;' /><span style='mso-element:field-end;'></span>";
                                        objLog.Body = Contenthtml;
                                        SendEmail(objLog, data);

                                    }
                                    SaveProcessedData(data);
                                }

                                if (exception.Rows.Count > 0)
                                {
                                    var FromDate = DateTime.ParseExact(exception.Rows[0]["Date of Joining GILAC (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture).ToString("dd MMM yyyy");
                                    var ToDate = DateTime.ParseExact(exception.Rows[data.Rows.Count]["Date of Joining GILAC (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture).ToString("dd MMM yyyy");
                                    ExceptionSubject = string.Format(ExceptionSubject, FromDate, ToDate);

                                    string Contenthtml = File.ReadAllText(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase).Replace("file:\\", "") + "\\Email HTML\\PayrollException.HTML");
                                    Contenthtml = string.Format(Contenthtml, ExceptionSubject, "<style type='text/css'> area:focus{ border: none; outline-style: none; -moz-outline-style:none; } map area { outline: none; }img { display: block; } a { cursor: pointer; }</style>", FromDate, ToDate);

                                    objLog.MailSubject = ExceptionSubject;
                                    objLog.FromMail = FromMailId;
                                    objLog.ModuleId = ModuleId;
                                    objLog.FromMailName = SenderName;
                                    //objLog.ToMail = "people.expcenter@godrejinds.com";
                                    objLog.ToMail = "amolkvjob@gmail.com";
                                    objLog.UserId = "";
                                    objLog.CcMail = CCMails;
                                    objLog.BccMail = BCCMails;
                                    objLog.HasError = "N";
                                    objLog.ErrorMsg = "";
                                    objLog.MailSent = "N";
                                    objLog.NotificationLogId = "0";
                                    string NotificationLogId = MailSentMailLog(objLog);
                                    objLog.NotificationLogId = NotificationLogId;
                                    Contenthtml += "<span style='mso-element:field-begin;'></span><img src='" + WebServiceURL + "InsertIJPEREFMailOpenLogs/" + objLog.UserId + "/" + NotificationLogId + "/" + objLog.ModuleId + "' alt='' class='outlookhide hideoutlookweb' style='display:none;' /><span style='mso-element:field-end;'></span>";
                                    objLog.Body = Contenthtml;
                                    SendEmail(objLog, exception);
                                }
                            }
                            else
                            {
                                var FromDate = DateTime.ParseExact(exception.Rows[0]["Date of Joining GILAC (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture).ToString("dd MMM yyyy");
                                var ToDate = DateTime.ParseExact(exception.Rows[data.Rows.Count]["Date of Joining GILAC (DD/MM/YYYY)"].ToString(), "dd/MM/yyyy", CultureInfo.InvariantCulture).ToString("dd MMM yyyy");
                                ExceptionSubject = string.Format(ExceptionSubject, FromDate, ToDate);

                                string Contenthtml = File.ReadAllText(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().GetName().CodeBase).Replace("file:\\", "") + "\\Email HTML\\PayrollException.HTML");
                                Contenthtml = string.Format(Contenthtml, ExceptionSubject, "<style type='text/css'> area:focus{ border: none; outline-style: none; -moz-outline-style:none; } map area { outline: none; }img { display: block; } a { cursor: pointer; }</style>", FromDate, ToDate);

                                objLog.MailSubject = ExceptionSubject;
                                objLog.FromMail = FromMailId;
                                objLog.ModuleId = ModuleId;
                                objLog.FromMailName = SenderName;
                                //objLog.ToMail = "people.expcenter@godrejinds.com";
                                objLog.ToMail = "amol07@live.com";
                                objLog.UserId = "";
                                objLog.CcMail = CCMails;
                                objLog.BccMail = BCCMails;
                                objLog.HasError = "N";
                                objLog.ErrorMsg = "";
                                objLog.MailSent = "N";
                                objLog.NotificationLogId = "0";
                                string NotificationLogId = MailSentMailLog(objLog);
                                objLog.NotificationLogId = NotificationLogId;
                                Contenthtml += "<span style='mso-element:field-begin;'></span><img src='" + WebServiceURL + "InsertIJPEREFMailOpenLogs/" + objLog.UserId + "/" + NotificationLogId + "/" + objLog.ModuleId + "' alt='' class='outlookhide hideoutlookweb' style='display:none;' /><span style='mso-element:field-end;'></span>";
                                objLog.Body = Contenthtml;
                                SendEmail(objLog, exception);
                            }
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("PEC Payroll Document Automation: " + ex.Message + System.DateTime.Now.ToString());
                }
            }
        }

        public static void SaveProcessedData(DataTable dt)
        {
            try
            {
                using (SqlConnection con = new SqlConnection(ConnectionString))
                {
                    using (SqlBulkCopy objbulk = new SqlBulkCopy(con))
                    {
                        objbulk.DestinationTableName = "Payroll_Godrej_Processed";

                        objbulk.ColumnMappings.Add("EMP NO", "EMP_NO");
                        objbulk.ColumnMappings.Add("EMP NAME (AS PER AADHAAR CARD)", "EMP_NAME");
                        objbulk.ColumnMappings.Add("PAY GROUP (Legal Entity)", "PAY_GROUP");
                        objbulk.ColumnMappings.Add("DOB (DD/MM/YYYY)", "DOB");
                        objbulk.ColumnMappings.Add("GENDER", "GENDER");
                        objbulk.ColumnMappings.Add("EMAIL ID (Company Email ID)", "EMAIL_ID");
                        objbulk.ColumnMappings.Add("USERNAME (SSO)", "USERNAME");
                        objbulk.ColumnMappings.Add("FATHER NAME", "FATHER_NAME");
                        objbulk.ColumnMappings.Add("DESIGNATION", "DESIGNATION");
                        objbulk.ColumnMappings.Add("SALARY GRADE (Linked to Reimbursement Limits)", "SALARY_GRADE");
                        objbulk.ColumnMappings.Add("DEPARTMENT", "DEPARTMENT");
                        objbulk.ColumnMappings.Add("LOCATION/CITY", "LOCATION");
                        objbulk.ColumnMappings.Add("COST CENTRE ( AS ON SF)", "COST_CENTRE");
                        objbulk.ColumnMappings.Add("Date of Joining GILAC (DD/MM/YYYY)", "DATE_OF_JOINING");
                        objbulk.ColumnMappings.Add("PAN NO", "PAN_NO");
                        objbulk.ColumnMappings.Add("Aadhar Card", "Aadhar_card");
                        objbulk.ColumnMappings.Add("Maritial Status", "Marital_Status");
                        objbulk.ColumnMappings.Add("PAYMENT MODE (B = Bank Transfer)", "PAYMENT_MODE");
                        objbulk.ColumnMappings.Add("BANK NAME", "Bank_Name");
                        objbulk.ColumnMappings.Add("BRANCH NAME", "Branch_Name");
                        objbulk.ColumnMappings.Add("BANK A/C NO", "Bank_Account_Number");
                        objbulk.ColumnMappings.Add("IFSC CODE", "IFSC_Code");
                        objbulk.ColumnMappings.Add("EDUCATION QUALIFICATION ( Applicable if 50 LPA Above)", "Education_Qualifications");
                        objbulk.ColumnMappings.Add("BASIC Per Month", "Basic_Monthly");
                        objbulk.ColumnMappings.Add("PF Per Month", "PF_Monthly");
                        objbulk.ColumnMappings.Add("Gratuity Per month", "Gratuity_Monthly");
                        objbulk.ColumnMappings.Add("Employee Retrials Per month", "Employee_Retirals_Monthly");
                        objbulk.ColumnMappings.Add("Educ Allow Per Month", "Education_Allowance_Monthly");
                        objbulk.ColumnMappings.Add("Sodexho Per month", "Sodexho_Monthly");
                        objbulk.ColumnMappings.Add("House Rent Allowance Per month", "HRA_Monthly");
                        objbulk.ColumnMappings.Add("LTA Per month", "LTA_Monthly");
                        objbulk.ColumnMappings.Add("Conveyance Per month", "Conveyance_Monthly");
                        objbulk.ColumnMappings.Add("Telephone Per month", "Telephone_Reimbursement_Monthly");
                        objbulk.ColumnMappings.Add("Driver Salary Per Month", "Driver_Salary_Allowance_Annum");
                        objbulk.ColumnMappings.Add("Supplementary Allow Per month", "Supplementary_Allowance_Monthly");
                        objbulk.ColumnMappings.Add("Flexible Compensation Per month", "Flexi_Monthly");
                        objbulk.ColumnMappings.Add("Ex Gratia /statutory Bonus Per month", "Ex_Gratia_statutory_Bonus_Monthly");
                        objbulk.ColumnMappings.Add("Total Fixed Per Annum", "Total_Fixed_Component");
                        objbulk.ColumnMappings.Add("PLVR I / PBFT I  (Per Annum)", "PLVR_I_PBFT_I_Annum");
                        objbulk.ColumnMappings.Add("PLVR C / PBFT C I (Per Annum)", "PLVR_C_PBFT_C_I_Annum");
                        objbulk.ColumnMappings.Add("Total PLVR (Per Annum)", "Total_PBFT_PLVR");
                        objbulk.ColumnMappings.Add("CTC (Per Annum)", "TOTAL_CTC");
                        objbulk.ColumnMappings.Add("Joining Bonus (Per Annum)", "Joining_Bonus_Monthly");
                        objbulk.ColumnMappings.Add("Clauses - All Additional Clauses with Amouts , if applicable", "Clause_Compensation_Information");
                        objbulk.ColumnMappings.Add("SBU", "Sub_Business_Unit");
                        objbulk.ColumnMappings.Add("Region", "Region");
                        objbulk.ColumnMappings.Add("Candidate Address", "Candidate_Address");
                        objbulk.ColumnMappings.Add("UAN No", "UAN");

                        con.Open();
                        objbulk.WriteToServer(dt);
                        con.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("SqlInsert: " + ex.Message + System.DateTime.Now.ToString());
                }
                var result = SchedulersLogInsertionAsync("PEC Payroll Processed data Sync Excecuted Failed", "SaveProcessedData", "Failed", ex.Message.ToString(), "", "999999");
            }
        }
        #endregion

        #region Archive Metadata
        static async Task OBDocsSqlArchiveAsync()
        {
            try
            {
                using (SqlConnection con = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("USP_PS_OBDocs_Archive", con))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        con.Open();
                        cmd.ExecuteNonQuery();
                        con.Close();
                        con.Dispose();
                    }
                }

                string[] files = Directory.GetFiles(InputPath);
                string fileName = string.Empty, destFile = string.Empty;
                foreach (string s in files)
                {
                    fileName = Path.GetFileName(s);
                    destFile = Path.Combine(ArchivePath, fileName);
                    File.Copy(s, destFile, true);
                    File.Delete(s);
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("SqlArchiveInsert: " + ex.Message + DateTime.Now.ToString());
                }
                var result = await SchedulersLogInsertionAsync("PEC Payroll Document Automation Data Sync Excecuted Failed", "OBDocsSqlArchiveAsync", "Failed", ex.Message.ToString(), "", "999999");
            }

        }
        #endregion

        #region Email Notification
        public static void SendEmail(EmailTemplate Logobj, DataTable dt = null)
        {
            try
            {
                MailMessage mailMessage = new MailMessage();
                MailAddress mailFrom = new MailAddress(Logobj.FromMail, Logobj.FromMailName);
                mailMessage.From = mailFrom;
                mailMessage.Subject = Logobj.MailSubject;
                mailMessage.Body = Logobj.Body;
                mailMessage.IsBodyHtml = true;
                //mailMessage.To.Add("godrejitetechsupport@godrejinds.com");
                mailMessage.To.Add("amol07@live.com");
                //if (!string.IsNullOrEmpty(Logobj.ToMail))
                //{
                //    foreach (var item in Logobj.ToMail.Split(','))
                //    {
                //        mailMessage.To.Add(item);
                //    }
                //}

                //if (!string.IsNullOrEmpty(Logobj.CcMail))
                //{
                //    foreach (var item in Logobj.CcMail.Split(','))
                //    {
                //        mailMessage.CC.Add(item);
                //    }
                //}

                //if (!string.IsNullOrEmpty(Logobj.BccMail))
                //{
                //    foreach (var item in Logobj.BccMail.Split(','))
                //    {
                //        mailMessage.Bcc.Add(item);
                //    }
                //}

                if (dt != null)
                {
                    if (dt.Rows.Count > 0)
                    {
                        string file = OutputPath + "\\PayrollInputData_" + DateTime.Now.ToString("ddMMMyyyy") + ".xlsx";
                        if (File.Exists(file))
                            File.Delete(file);
                        ExportDataTableToExcel(dt);
                        mailMessage.Attachments.Add(new Attachment(file));
                    }
                }


                SmtpClient smtp = new SmtpClient();
                try
                {
                    smtp.Send(mailMessage);
                    Logobj.MailSent = "Y";
                    Logobj.ErrorMsg = "Mail sent successfully.";

                    using (StreamWriter sw = File.AppendText(LogPath))
                    {
                        sw.WriteLine("Email Status: " + Logobj.ToMail + " " + System.DateTime.Now.ToString() + " Email sent successfully");
                    }
                }
                catch (Exception ex)
                {
                    Logobj.MailSent = "N";
                    Logobj.HasError = "Y";
                    Logobj.ErrorMsg = "Mail sending Error : " + ex.StackTrace.ToString() + ex.Message;
                    using (StreamWriter sw = File.AppendText(LogPath))
                    {
                        sw.WriteLine("Email Status: " + Logobj.ToMail + " " + System.DateTime.Now.ToString() + " Error" + ex.ToString());
                    }
                }
                finally
                {
                    var result = MailSentMailLog(Logobj);
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("SendEmail: " + ex.Message + System.DateTime.Now.ToString());
                }
            }
        }
        private static void ExportDataTableToExcel(DataTable dataTable)
        {
            string file = OutputPath + "\\PayrollInputData_" + DateTime.Now.ToString("ddMMMyyyy") + ".xlsx";
            using (XLWorkbook wb = new XLWorkbook())
            {
                wb.Worksheets.Add(dataTable, "Worksheet");
                wb.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
                wb.Style.Font.Bold = true;
                wb.SaveAs(file);
            }
        }
        public static string MailSentMailLog(EmailTemplate Logobj)
        {
            try
            {
                DataTable dtMailLogData = new DataTable();
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("USP_MailLogInsertion", connection))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;
                        cmd.Parameters.AddWithValue("@NotificationLogId", Logobj.NotificationLogId);
                        cmd.Parameters.AddWithValue("@Module", Logobj.ModuleId);
                        cmd.Parameters.AddWithValue("@EmployeeId", Logobj.UserId);
                        cmd.Parameters.AddWithValue("@MailSubject", Logobj.MailSubject);
                        cmd.Parameters.AddWithValue("@ToMail", Logobj.ToMail);
                        cmd.Parameters.AddWithValue("@CcMail", Logobj.CcMail);
                        cmd.Parameters.AddWithValue("@BccMail", Logobj.BccMail);
                        cmd.Parameters.AddWithValue("@MailSent", Logobj.MailSent);
                        cmd.Parameters.AddWithValue("@MailSentTime", DateTime.Now);
                        cmd.Parameters.AddWithValue("@HasError", Logobj.HasError);
                        cmd.Parameters.AddWithValue("@ErrorMsg", Logobj.ErrorMsg);
                        cmd.Parameters.AddWithValue("@RoutedOn", DateTime.Now);
                        cmd.Parameters.AddWithValue("@InsertedBy", "999999");
                        cmd.Parameters.AddWithValue("@FromMail", Logobj.FromMail);
                        connection.Open();
                        using (SqlDataAdapter adp = new SqlDataAdapter(cmd))
                        {
                            using (DataSet dsMailLogData = new DataSet())
                            {
                                adp.Fill(dsMailLogData);
                                if (dsMailLogData.Tables.Count > 0)
                                    if (dsMailLogData.Tables[0] != null)
                                        dtMailLogData = dsMailLogData.Tables[0];
                                    else Logobj.NotificationLogId = "0";
                            }
                        }
                        connection.Close();
                    }
                }

                if (dtMailLogData.Rows.Count > 0)
                    return dtMailLogData.Rows[0][0].ToString();
                else return Logobj.NotificationLogId;
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.AppendText(ErrorLogPath))
                {
                    sw.WriteLine("Mail Sent MailLog Error : " + ex.StackTrace.ToString() + ex.Message + " Time:" + DateTime.Now.ToString());
                }
                return "0";
            }
        }
        #endregion
        private static async Task<string> SchedulersLogInsertionAsync(string Description, string ApplicationName, string Status, string ErrorMessage, string DataStoredPath, string CreatedBy)
        {
            string Result = string.Empty;
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    string Baseurl = WebServiceURL + "SchedulersLogsInsertion/" + ModuleId + "/"
                    + Description + "/"
                    + ApplicationName + "/"
                    + Status + "/"
                    + ErrorMessage.Replace(@"\", "~").Replace(@"'", "`").Replace("/", "~~") + " / "
                    + DataStoredPath.Replace(@"\", "~").Replace(@"'", "`") + "/"
                    + CreatedBy;

                    client.BaseAddress = new Uri(Baseurl);
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
                    HttpResponseMessage Res = await client.GetAsync("");

                    if (Res.IsSuccessStatusCode)
                    {
                        var Response = Res.Content.ReadAsStringAsync().Result;
                        JObject joResponse = JObject.Parse(Response);
                        Result = joResponse["SchedulersLogsInsertionResult"].ToString();
                    }
                    else Result = "Failed";

                }
            }
            catch (Exception)
            {
                Result = "Failed";
            }

            return Result;
        }



    }
}
