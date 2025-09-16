using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using NPOI.XSSF.UserModel;

namespace HRUser_TO_SOP_DB
{
    /// <summary>
    /// 主程式類別 - 負責將HR人員資料從Excel匯入到SOP資料庫
    /// </summary>
    class Program
    {
        #region 欄位定義
        /// <summary>
        /// Excel檔案路徑-讀取設定檔中的Excel路徑
        /// </summary>
        private static string _xlsPath = ConfigurationManager.AppSettings["XLS_PATH"];

        /// <summary>
        /// 資料庫連線字串
        /// </summary>
        private static readonly string _connectionString = ConfigurationManager.AppSettings["CNN_TEXT"];

        /// <summary>
        /// 目標資料表名稱
        /// </summary>
        private const string TARGET_TABLE = "i_Factory_EversunUser_Tabel";
        #endregion

        #region 主程式
        /// <summary>
        /// 程式進入點
        /// </summary>
        /// <param name="args">命令列參數</param>
        static void Main(string[] args)
        {
            Console.WriteLine("資料寫入中.....請稍候");

            try
            {
                if (string.IsNullOrEmpty(_xlsPath))
                {
                    throw new ArgumentException("Excel檔案路徑未設定，請檢查App.config中的XLS_PATH設定");
                }

                // 處理Excel檔案
                ProcessExcelFiles();

                Console.WriteLine("\n\n寫入完畢，按任意鍵關閉程式！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"錯誤：{ex.Message}");
                Console.WriteLine($"堆疊追蹤：{ex.StackTrace}");
            }
            finally
            {
                Console.ReadKey();
            }
        }
        #endregion

        #region 檔案處理方法
        /// <summary>
        /// 處理指定目錄下的所有Excel檔案
        /// </summary>
        private static void ProcessExcelFiles()
        {
            DirectoryInfo xlsDir = new DirectoryInfo(_xlsPath);
            if (!xlsDir.Exists)
            {
                throw new DirectoryNotFoundException($"找不到指定的目錄：{_xlsPath}");
            }

            // 取得所有.xlsx檔案
            FileInfo[] xlsFiles = xlsDir.GetFiles("*.xlsx");
            if (xlsFiles.Length == 0)
            {
                Console.WriteLine("未找到任何Excel檔案，請確認路徑是否正確");
                return;
            }

            Console.WriteLine($"找到 {xlsFiles.Length} 個Excel檔案，開始處理...");
            int processedFiles = 0;
            int processedRecords = 0;

            foreach (FileInfo xlsFile in xlsFiles)
            {
                try
                {
                    Console.WriteLine($"\n處理檔案：{xlsFile.Name}");

                    // 載入Excel檔案到DataTable
                    DataTable dt = LoadExcelAsDataTable(xlsFile.FullName);

                    // 處理資料表中的每一筆資料
                    int fileRecords = ProcessDataTable(dt);

                    processedRecords += fileRecords;
                    processedFiles++;

                    Console.WriteLine($"檔案 {xlsFile.Name} 處理完成，共處理 {fileRecords} 筆記錄");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"處理檔案 {xlsFile.Name} 時發生錯誤：{ex.Message}");
                }
            }

            Console.WriteLine($"\n所有檔案處理完成，共處理 {processedFiles} 個檔案，{processedRecords} 筆記錄");
        }

        /// <summary>
        /// 處理資料表中的每一筆資料
        /// </summary>
        /// <param name="dataTable">包含員工資料的資料表</param>
        /// <returns>處理的記錄數</returns>
        private static int ProcessDataTable(DataTable dataTable)
        {
            if (dataTable == null || dataTable.Rows.Count == 0)
            {
                return 0;
            }

            int processedCount = 0;

            foreach (DataRow row in dataTable.Rows)
            {
                // 取得員工ID和姓名
                string userId = row[0]?.ToString().Trim();
                string userName = row[1]?.ToString().Trim();

                // 跳過空值
                if (string.IsNullOrEmpty(userId) || string.IsNullOrEmpty(userName))
                {
                    continue;
                }

                try
                {
                    // 檢查資料庫中是否已存在此員工
                    if (!UserExists(userId))
                    {
                        // 新增員工資料到資料庫
                        InsertUser(userId, userName);
                        processedCount++;
                        Console.WriteLine($"新增員工：{userId}, {userName}");
                    }
                    //else
                    //{
                    //    Console.WriteLine($"員工已存在，跳過：{userId}");
                    //}
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"處理員工 {userId} 時發生錯誤：{ex.Message}");
                }
            }

            return processedCount;
        }
        #endregion

        #region 資料庫操作方法
        /// <summary>
        /// 檢查指定的員工ID是否已存在於資料庫
        /// </summary>
        /// <param name="userId">員工ID</param>
        /// <returns>存在返回true，否則返回false</returns>
        private static bool UserExists(string userId)
        {
            string sql = $"SELECT User_ID FROM {TARGET_TABLE} WHERE User_ID = @userId";
            SqlParameter[] parameters = { new SqlParameter("@userId", userId) };

            using (SqlDataReader reader = ExecuteReader(sql, CommandType.Text, parameters))
            {
                return reader.Read();
            }
        }

        /// <summary>
        /// 新增員工資料到資料庫
        /// </summary>
        /// <param name="userId">員工ID</param>
        /// <param name="userName">員工姓名</param>
        /// <returns>影響的行數</returns>
        private static int InsertUser(string userId, string userName)
        {
            string sql = $"INSERT INTO {TARGET_TABLE} VALUES (@userId, @userName)";
            SqlParameter[] parameters =
            {
                new SqlParameter("@userId", userId.Length==4?"0"+userId:userId),
                new SqlParameter("@userName", userName)
            };

            return ExecuteNonQuery(sql, CommandType.Text, parameters);
        }

        /// <summary>
        /// 執行不返回結果的SQL命令
        /// </summary>
        /// <param name="sql">SQL命令文本</param>
        /// <param name="cmdType">命令類型</param>
        /// <param name="parameters">SQL參數</param>
        /// <returns>影響的行數</returns>
        public static int ExecuteNonQuery(string sql, CommandType cmdType, params SqlParameter[] parameters)
        {
            if (string.IsNullOrEmpty(sql))
            {
                throw new ArgumentNullException(nameof(sql), "SQL命令不能為空");
            }

            using (SqlConnection connection = new SqlConnection(_connectionString))
            using (SqlCommand command = new SqlCommand(sql, connection))
            {
                command.CommandType = cmdType;

                if (parameters != null && parameters.Length > 0)
                {
                    command.Parameters.AddRange(parameters);
                }

                connection.Open();
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// 執行返回SqlDataReader的SQL命令
        /// </summary>
        /// <param name="sql">SQL命令文本</param>
        /// <param name="cmdType">命令類型</param>
        /// <param name="parameters">SQL參數</param>
        /// <returns>SqlDataReader對象</returns>
        public static SqlDataReader ExecuteReader(string sql, CommandType cmdType, params SqlParameter[] parameters)
        {
            if (string.IsNullOrEmpty(sql))
            {
                throw new ArgumentNullException(nameof(sql), "SQL命令不能為空");
            }

            SqlConnection connection = new SqlConnection(_connectionString);
            try
            {
                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    command.CommandType = cmdType;

                    if (parameters != null && parameters.Length > 0)
                    {
                        command.Parameters.AddRange(parameters);
                    }

                    connection.Open();
                    // 使用CloseConnection行為，確保在關閉Reader時同時關閉連接
                    return command.ExecuteReader(CommandBehavior.CloseConnection);
                }
            }
            catch
            {
                connection.Close();
                connection.Dispose();
                throw;
            }
        }
        #endregion

        #region Excel處理方法
        /// <summary>
        /// 將Excel檔案載入為DataTable
        /// </summary>
        /// <param name="xlsFilename">Excel檔案路徑</param>
        /// <returns>包含Excel資料的DataTable</returns>
        public static DataTable LoadExcelAsDataTable(string xlsFilename)
        {
            if (string.IsNullOrEmpty(xlsFilename))
            {
                throw new ArgumentNullException(nameof(xlsFilename), "Excel檔案路徑不能為空");
            }

            FileInfo fileInfo = new FileInfo(xlsFilename);
            if (!fileInfo.Exists)
            {
                throw new FileNotFoundException("找不到指定的Excel檔案", xlsFilename);
            }

            using (FileStream fileStream = new FileStream(fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                // 根據檔案副檔名決定使用哪種工作簿
                IWorkbook workbook;
                if (fileInfo.Extension.Equals(".xlsx", StringComparison.OrdinalIgnoreCase))
                {
                    workbook = new XSSFWorkbook(fileStream); // Excel 2007+ 格式
                }
                else if (fileInfo.Extension.Equals(".xls", StringComparison.OrdinalIgnoreCase))
                {
                    workbook = new HSSFWorkbook(fileStream); // Excel 97-2003 格式
                }
                else
                {
                    throw new NotSupportedException("不支援的Excel檔案格式，僅支援.xls和.xlsx格式");
                }

                // 取得第一個工作表
                ISheet sheet = workbook.GetSheetAt(0);
                if (sheet == null)
                {
                    throw new InvalidOperationException("Excel檔案中沒有工作表");
                }

                return ConvertSheetToDataTable(sheet);
            }
        }

        /// <summary>
        /// 將Excel工作表轉換為DataTable
        /// </summary>
        /// <param name="sheet">Excel工作表</param>
        /// <returns>包含工作表資料的DataTable</returns>
        private static DataTable ConvertSheetToDataTable(ISheet sheet)
        {
            DataTable dataTable = new DataTable();

            // 取得標題列
            IRow headerRow = sheet.GetRow(0);
            if (headerRow == null)
            {
                throw new InvalidOperationException("Excel工作表中沒有標題列");
            }

            // 建立資料表欄位
            int cellCount = headerRow.LastCellNum;
            for (int i = headerRow.FirstCellNum; i < cellCount; i++)
            {
                ICell cell = headerRow.GetCell(i);
                if (cell != null)
                {
                    string columnName = cell.StringCellValue;
                    if (string.IsNullOrEmpty(columnName))
                    {
                        columnName = $"Column{i}";
                    }
                    dataTable.Columns.Add(new DataColumn(columnName));
                }
                else
                {
                    dataTable.Columns.Add(new DataColumn($"Column{i}"));
                }
            }

            // 處理資料列
            for (int i = sheet.FirstRowNum + 1; i <= sheet.LastRowNum; i++)
            {
                IRow row = sheet.GetRow(i);
                if (row == null) continue;

                DataRow dataRow = dataTable.NewRow();

                // 處理每個儲存格
                for (int j = row.FirstCellNum; j < cellCount; j++)
                {
                    if (j < 0) continue;

                    ICell cell = row.GetCell(j);
                    if (cell != null)
                    {
                        // 根據儲存格類型設定值
                        switch (cell.CellType)
                        {
                            case CellType.Numeric:
                                // 檢查是否為日期
                                if (DateUtil.IsCellDateFormatted(cell))
                                {
                                    dataRow[j] = cell.DateCellValue;
                                }
                                else
                                {
                                    dataRow[j] = cell.NumericCellValue;
                                }
                                break;
                            case CellType.String:
                                dataRow[j] = cell.StringCellValue;
                                break;
                            case CellType.Boolean:
                                dataRow[j] = cell.BooleanCellValue;
                                break;
                            case CellType.Formula:
                                // 嘗試獲取公式計算結果
                                try
                                {
                                    dataRow[j] = cell.StringCellValue;
                                }
                                catch
                                {
                                    try
                                    {
                                        dataRow[j] = cell.NumericCellValue;
                                    }
                                    catch
                                    {
                                        dataRow[j] = cell.CellFormula;
                                    }
                                }
                                break;
                            case CellType.Blank:
                                dataRow[j] = DBNull.Value;
                                break;
                            default:
                                dataRow[j] = cell.ToString();
                                break;
                        }
                    }
                }

                dataTable.Rows.Add(dataRow);
            }

            return dataTable;
        }
        #endregion
    }
}
