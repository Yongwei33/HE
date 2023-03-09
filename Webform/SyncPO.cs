using System;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Dapper.Contrib.Extensions;
using Lib.Sync.Model;
using System.Collections.Generic;
using Ede.Uof.Utility.Log;

namespace Lib.Sync.Sync
{
    public class SyncPO : Ede.Uof.Utility.Data.BasePersistentObject
    {
        // 取得部門資料
        public List<SW_HR_DEPT> Get_DEPT()
        {
            using (var cnn = GetConnection())
            {
                List<SW_HR_DEPT> listResult = new List<SW_HR_DEPT>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    var dp = new DynamicParameters();
                    strSQL.Append(" SELECT Code, Name, Parent, Superior ");
                    strSQL.Append(" FROM dbo.SW_HR_DEPT ");
                    string H = "H";
                    H = $"{H}%";
                    strSQL.Append(" WHERE Code LIKE @H ");
                    dp.Add("@H", H);
                    strSQL.Append(" ORDER BY Code ");

                    listResult = cnn.Query<SW_HR_DEPT>(strSQL.ToString(), dp).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        // 取得職級
        public List<SW_HR_JOBTITLE> Get_TITLE()
        {
            using (var cnn = GetConnection())
            {
                List<SW_HR_JOBTITLE> listResult = new List<SW_HR_JOBTITLE>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT * ");
                    strSQL.Append(" FROM dbo.SW_HR_JOBTITLE ");


                    listResult = cnn.Query<SW_HR_JOBTITLE>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得職務
        /// </summary>
        public List<SW_HR_FUNCTION> Get_POSITON()
        {
            using (var cnn = GetConnection())
            {
                List<SW_HR_FUNCTION> listResult = new List<SW_HR_FUNCTION>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT * ");
                    strSQL.Append(" FROM dbo.SW_HR_FUNCTION ");

                    listResult = cnn.Query<SW_HR_FUNCTION>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        // 取得員工資料
        public List<SW_HR_ACCOUNT> Get_ACCOUNT()
        {
            using (var cnn = GetConnection())
            {
                List<SW_HR_ACCOUNT> listResult = new List<SW_HR_ACCOUNT>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT * ");
                    strSQL.Append(" FROM dbo.SW_HR_ACCOUNT ");
                    strSQL.Append(" WHERE EmployeeState = '1' ");

                    listResult = cnn.Query<SW_HR_ACCOUNT>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        // 取得UOF所有職級
        public List<TB_EB_JOB_TITLE> GetJOB_TITLE()
        {
            using (var cnn = GetConnection())
            {
                List<TB_EB_JOB_TITLE> listResult = new List<TB_EB_JOB_TITLE>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT RANK, TITLE_ID, TITLE_NAME, MAX_FILE_SIZE ");
                    strSQL.Append(" FROM TB_EB_JOB_TITLE ");

                    listResult = cnn.Query<TB_EB_JOB_TITLE>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        // 取得UOF所有組織
        public List<TB_EB_GROUP> GetGROUP()
        {
            using (var cnn = GetConnection())
            {
                List<TB_EB_GROUP> listResult = new List<TB_EB_GROUP>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT [GROUP_ID], [GROUP_NAME], [PARENT_GROUP_ID], [GROUP_CODE], [ACTIVE] ");
                    strSQL.Append(" FROM TB_EB_GROUP ");

                    listResult = cnn.Query<TB_EB_GROUP>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        // 取得UOF人員所屬部門及職稱
        internal List<TB_EB_EMPL_DEP> GetEMPL_DEP(string userGuid)
        {
            using (var cnn = GetConnection())
            {
                List<TB_EB_EMPL_DEP> listResult = new List<TB_EB_EMPL_DEP>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT GROUP_ID, USER_GUID, TITLE_ID, ORDERS ");
                    strSQL.Append(" FROM TB_EB_EMPL_DEP ");
                    strSQL.Append(" WHERE USER_GUID = @USER_GUID ");
                    strSQL.Append(" ORDER BY ORDERS ");

                    listResult = cnn.Query<TB_EB_EMPL_DEP>(strSQL.ToString(), new { @USER_GUID = userGuid }).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        // 取得所有UOF人員所屬部門及職稱
        internal List<TB_EB_EMPL_DEP> GetAllEMPL_DEP()
        {
            using (var cnn = GetConnection())
            {
                List<TB_EB_EMPL_DEP> listResult = new List<TB_EB_EMPL_DEP>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT GROUP_ID, USER_GUID, TITLE_ID, ORDERS ");
                    strSQL.Append(" FROM TB_EB_EMPL_DEP ");
                    strSQL.Append(" WHERE ORDERS <> 0 ");
                    strSQL.Append(" ORDER BY USER_GUID ");

                    listResult = cnn.Query<TB_EB_EMPL_DEP>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        //取得UOF部門主管
        public List<TB_EB_EMPL_FUNC> GetUOF_Superior(string groupId)
        {
            using (var cnn = GetConnection())
            {
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT GROUP_ID, USER_GUID, FUNC_ID ");
                    strSQL.Append(" FROM TB_EB_EMPL_FUNC ");
                    strSQL.Append(" WHERE GROUP_ID = @GROUP_ID AND FUNC_ID = 'Superior' ");

                    List<TB_EB_EMPL_FUNC> listResult = cnn.Query<TB_EB_EMPL_FUNC>(strSQL.ToString(), new { GROUP_ID = groupId }).ToList();
                    return listResult;
                }
                catch
                {
                    throw;
                }
            }
        }

        //取得UOF部門主管
        public List<TB_EB_EMPL_FUNC> GetUOF_SuperiorGUID(string GROUP_ID, string USER_GUID)
        {
            using (var cnn = GetConnection())
            {
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT GROUP_ID, USER_GUID, FUNC_ID ");
                    strSQL.Append(" FROM TB_EB_EMPL_FUNC ");
                    strSQL.Append(" WHERE GROUP_ID = @GROUP_ID AND FUNC_ID = 'Superior' AND USER_GUID = @USER_GUID ");

                    List<TB_EB_EMPL_FUNC> listResult = cnn.Query<TB_EB_EMPL_FUNC>(strSQL.ToString(), new { GROUP_ID, USER_GUID }).ToList();
                    return listResult;
                }
                catch
                {
                    throw;
                }
            }
        }

        //取得UOF簽核人員
        public List<TB_EB_EMPL_FUNC> GetUOF_Signer(string USER_GUID, string GROUP_ID)
        {
            using (var cnn = GetConnection())
            {
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT GROUP_ID, USER_GUID, FUNC_ID ");
                    strSQL.Append(" FROM TB_EB_EMPL_FUNC ");
                    strSQL.Append(" WHERE GROUP_ID = @GROUP_ID AND FUNC_ID = 'Signer' AND USER_GUID = @USER_GUID ");

                    List<TB_EB_EMPL_FUNC> listResult = cnn.Query<TB_EB_EMPL_FUNC>(strSQL.ToString(), new { GROUP_ID, USER_GUID }).ToList();
                    return listResult;
                }
                catch
                {
                    throw;
                }
            }
        }

        // 員工部門異動
        /// 1.先刪除前部門的職務、包含員工、簽核人員及主管職務(若該員工為部門主管)
        /// 2.新增新部門的主要職務、簽核人員
        /// 更新員工主要職務(來自HRA_POSITION)
        /// <param name="oldUserGuid">User Guid</param>
        /// <param name="preGroupId">前次同步所在的部門GroupId</param>
        /// <param name="syncGroupId">當次同步所在的部門GroupId</param>
        /// <param name="funcId"></前次同步所屬的職務GroupId>
        internal void UpdateEmp_New_Main_GroupFunc(string oldUserGuid, string preGroupId, string syncGroupId, string funcId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" DELETE FROM TB_EB_EMPL_FUNC WHERE USER_GUID = @USER_GUID AND GROUP_ID = @PRE_GROUP_ID"); //刪除前部門的職務
                strSQL.Append(" INSERT INTO TB_EB_EMPL_FUNC (GROUP_ID, USER_GUID, FUNC_ID) "); //新增主要部門職務
                strSQL.Append(" VALUES(@SYNC_GROUP_ID, @USER_GUID, @FUNC_ID) ");

                this.m_db.AddParameter("@USER_GUID", oldUserGuid);
                this.m_db.AddParameter("@PRE_GROUP_ID", preGroupId);
                this.m_db.AddParameter("@SYNC_GROUP_ID", syncGroupId);
                this.m_db.AddParameter("@FUNC_ID", funcId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        // 更新員工主要部門及主要職稱
        internal void UpdateEmp_Main_Dept_Title(string oldUserGuid, string groupId, string titleId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                //strSQL.Append(" DELETE FROM TB_EB_EMPL_DEP WHERE USER_GUID = @USER_GUID AND GROUP_ID = @GROUP_ID ");
                strSQL.Append(" UPDATE TB_EB_EMPL_DEP SET GROUP_ID=@GROUP_ID, TITLE_ID=@TITLE_ID WHERE USER_GUID = @USER_GUID AND ORDERS = @ORDERS ");
                //strSQL.Append(" DELETE FROM TB_EB_EMPL_FUNC WHERE USER_GUID = @USER_GUID AND GROUP_ID = @GROUP_ID ");
                //strSQL.Append(" INSERT INTO TB_EB_EMPL_DEP (USER_GUID, GROUP_ID, TITLE_ID, ORDERS) ");
                //strSQL.Append(" VALUES(@USER_GUID, @GROUP_ID, @TITLE_ID, @ORDERS) ");

                this.m_db.AddParameter("@USER_GUID", oldUserGuid);
                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@TITLE_ID", titleId);
                this.m_db.AddParameter("@ORDERS", 0);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }

        }

        // 更新員工主要職務
        /// <param name="oldUserGuid"></param>
        /// <param name="groupId"></param>
        /// <param name="funcId"></param>
        internal void UpdateEmp_Main_GroupFunc(string oldUserGuid, string groupId, string funcId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" DELETE FROM TB_EB_EMPL_FUNC WHERE GROUP_ID = @GROUP_ID AND USER_GUID = @USER_GUID ");
                strSQL.Append(" AND FUNC_ID <> 'Signer' AND FUNC_ID <> 'Superior' ");
                strSQL.Append(" INSERT INTO TB_EB_EMPL_FUNC (GROUP_ID, USER_GUID, FUNC_ID) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, @FUNC_ID) ");

                this.m_db.AddParameter("@USER_GUID", oldUserGuid);
                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@FUNC_ID", funcId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        // 更新員工異動新部門資訊
        /// <param name="oldUserGuid"></param>
        /// <param name="preSyncGroupId"></param>
        /// <param name="curSyncGroupId"></param>
        /// <param name="titleId"></param>
        internal void UpdateEmp_NewMain_Dept_Title(string oldUserGuid, string preSyncGroupId, string curSyncGroupId, string titleId)
        {
            this.m_db.BeginTransaction();
            try
            {
                //確保不能有同樣ORDERS的資料,要先刪除原有的ORDERS 0:主要部門 1:兼任1 2:兼任2
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" DELETE FROM TB_EB_EMPL_DEP WHERE USER_GUID = @USER_GUID AND GROUP_ID = @SYNC_GROUP_ID "); //若兼任部門變為主要部門，先刪除兼任部門
                strSQL.Append(" DELETE FROM TB_EB_EMPL_DEP WHERE USER_GUID = @USER_GUID AND ORDERS = @ORDERS "); //刪除主要部門
                strSQL.Append(" DELETE FROM TB_EB_EMPL_FUNC WHERE USER_GUID = @USER_GUID AND GROUP_ID = @PRE_GROUP_ID AND FUNC_ID = 'Signer' "); //刪除主要部門簽核職務
                strSQL.Append(" INSERT INTO TB_EB_EMPL_DEP (USER_GUID, GROUP_ID, TITLE_ID, ORDERS) "); //加入調動後的部門
                strSQL.Append(" VALUES(@USER_GUID, @SYNC_GROUP_ID, @TITLE_ID, @ORDERS) ");

                this.m_db.AddParameter("@USER_GUID", oldUserGuid);
                this.m_db.AddParameter("@PRE_GROUP_ID", preSyncGroupId);
                this.m_db.AddParameter("@SYNC_GROUP_ID", curSyncGroupId);
                this.m_db.AddParameter("@TITLE_ID", titleId);
                this.m_db.AddParameter("@ORDERS", 0);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }

        }

        /// <summary>
        /// 移除員工的部門主管及簽核人員職務
        /// </summary>
        internal void DeleteEMPL_DEPTByUserGuid_GROUP_ID(string userGuid, string groupId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();

                strSQL.Append(" DELETE FROM TB_EB_EMPL_DEP ");
                strSQL.Append(" WHERE USER_GUID = @USER_GUID AND GROUP_ID = @GROUP_ID ");


                this.m_db.AddParameter("@USER_GUID", userGuid);
                this.m_db.AddParameter("@GROUP_ID", groupId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        internal void DeleteEMPL_SUPERIOR_ByGROUP_ID(string userGuid, string groupId, int orders)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();

                strSQL.Append(" DELETE FROM TB_EB_EMPL_FUNC ");
                strSQL.Append("  WHERE USER_GUID = @USER_GUID AND GROUP_ID = @GROUP_ID");
                if(orders == 0)
                {
                    strSQL.Append(" AND FUNC_ID = 'Superior'");
                }

                this.m_db.AddParameter("@USER_GUID", userGuid);
                this.m_db.AddParameter("@GROUP_ID", groupId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 新增兼任部門及簽核人員職務
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="userGuid"></param>
        /// <param name="titleId"></param>
        /// <param name="orders"></param>
        internal void InsertEMP_PT_DEP_FUNC(string groupId, string userGuid, string titleId, int orders)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" INSERT INTO TB_EB_EMPL_DEP ([GROUP_ID], [USER_GUID], [TITLE_ID], [ORDERS]) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, @TITLE_ID, @ORDERS) ");
                strSQL.Append(" INSERT INTO TB_EB_EMPL_FUNC ([GROUP_ID], [USER_GUID], [FUNC_ID]) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, 'Signer') ");

                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@USER_GUID", userGuid);
                this.m_db.AddParameter("@TITLE_ID", titleId);
                this.m_db.AddParameter("@ORDERS", orders);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 新增兼任部門
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="userGuid"></param>
        /// <param name="titleId"></param>
        /// <param name="orders"></param>
        internal void InsertEMP_PT_DEP(string groupId, string userGuid, string titleId, int orders)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" INSERT INTO TB_EB_EMPL_DEP ([GROUP_ID], [USER_GUID], [TITLE_ID], [ORDERS]) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, @TITLE_ID, @ORDERS) ");

                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@USER_GUID", userGuid);
                this.m_db.AddParameter("@TITLE_ID", titleId);
                this.m_db.AddParameter("@ORDERS", orders);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 新增兼任部門職務
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="userGuid"></param>
        /// <param name="titleId"></param>
        /// <param name="orders"></param>
        internal void InsertEMP_PT_FUNC(string groupId, string userGuid, string functionId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" INSERT INTO TB_EB_EMPL_FUNC ([GROUP_ID], [USER_GUID], [FUNC_ID]) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, @FUNCTION_ID) ");

                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@USER_GUID", userGuid);
                this.m_db.AddParameter("@FUNCTION_ID", functionId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 變更部門主管
        /// </summary>
        internal void InsertJobFunction(string groupId, string userGuid)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" INSERT INTO TB_EB_EMPL_FUNC ([GROUP_ID], [USER_GUID], [FUNC_ID]) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, 'Superior') ");

                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@USER_GUID", userGuid);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 賦予簽核人員職務
        /// </summary>
        internal void InsertJobFunctionSigner(string userGuid, string groupId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" INSERT INTO TB_EB_EMPL_FUNC ([GROUP_ID], [USER_GUID], [FUNC_ID]) ");
                strSQL.Append(" VALUES(@GROUP_ID, @USER_GUID, 'Signer') ");

                this.m_db.AddParameter("@GROUP_ID", groupId);
                this.m_db.AddParameter("@USER_GUID", userGuid);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 刪除員工所有兼任部門
        /// </summary>
        internal void DeleteEmp_Dept(string oldUserGuid, string mainGroupId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" DELETE TB_EB_EMPL_DEP WHERE USER_GUID = @USER_GUID AND ORDERS <> 0 ");
                strSQL.Append(" DELETE [dbo].[TB_EB_EMPL_FUNC] ");
                strSQL.Append(" WHERE [USER_GUID] = @USER_GUID AND [FUNC_ID] = 'Signer' AND [GROUP_ID] <> @GROUP_ID ");

                this.m_db.AddParameter("@USER_GUID", oldUserGuid);
                this.m_db.AddParameter("@GROUP_ID", mainGroupId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 刪除員工單個兼任部門
        /// </summary>
        internal void DeleteOneEmp_Dept(string userGuid, string groupId)
        {
            this.m_db.BeginTransaction();
            try
            {
                StringBuilder strSQL = new StringBuilder();
                strSQL.Append(" DELETE TB_EB_EMPL_DEP ");
                strSQL.Append(" WHERE [USER_GUID] = @USER_GUID AND [GROUP_ID] = @GROUP_ID ");

                this.m_db.AddParameter("@USER_GUID", userGuid);
                this.m_db.AddParameter("@GROUP_ID", groupId);

                this.m_db.ExecuteNonQuery(strSQL.ToString());
                this.m_db.CommitTransaction();
            }
            catch
            {
                this.m_db.RollbackTransaction();
                throw;
            }
        }

        /// <summary>
        /// 取得UOF所有員工
        /// </summary>
        public List<TB_EB_USER> GetUSER()
        {
            using (var cnn = GetConnection())
            {
                List<TB_EB_USER> listResult = new List<TB_EB_USER>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT USER_GUID, ACCOUNT, NAME, IS_SUSPENDED ");
                    strSQL.Append(" FROM TB_EB_USER ");

                    listResult = cnn.Query<TB_EB_USER>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得兼任部門資訊
        /// </summary>
        public List<SW_HR_CONCURREN> GetCONCURREN(string EndDate)
        {
            using (var cnn = GetConnection())
            {
                List<SW_HR_CONCURREN> listResult = new List<SW_HR_CONCURREN>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT DISTINCT Account, PartJobDepCode, PartJobDepName, PartJobJobTitleName, PartJobFunctionName, BeginDate, EndDate, IsEffective ");
                    strSQL.Append(" FROM SW_HR_CONCURREN ");
                    strSQL.Append(" WHERE IsEffective = '1' AND EndDate > @EndDate ");
                    strSQL.Append(" ORDER BY Account ");

                    listResult = cnn.Query<SW_HR_CONCURREN>(strSQL.ToString(), new { EndDate }).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 比對停用員工
        /// </summary>
        /// <param name="srcEmpno">員工工號</param>
        public List<SW_HR_ACCOUNT> Get_DIS_EMP(string Account)
        {
            using (var cnn = GetConnection())
            {
                List<SW_HR_ACCOUNT> listResult = null;
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append(" SELECT * ");
                    strSQL.Append(" FROM dbo.SW_HR_ACCOUNT ");
                    strSQL.Append(" WHERE EmployeeState <> '1' AND Account = @Account ");

                    listResult = cnn.Query<SW_HR_ACCOUNT>(strSQL.ToString(), new { Account }).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得所有員工新資料
        /// </summary>
        /*public List<SW_HR_ACCOUNT> GetSW_HR_ACCOUNT()
        {
            using (var cnn = GetANDESConnection())
            {
                List<SW_HR_ACCOUNT> listResult = new List<SW_HR_ACCOUNT>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append("SELECT  A.Code'Account', A.CnName'Name', B.Name'Title', C.Name'Function', A.EMAIL'Email', D.Code'Dept_Code', D.Name'Department', E.ScName'Sex', ");
                    strSQL.Append("F.NAME'Location',A.BIRTHDATE'BirthDay', A.DATE'ArriveDay', CASE WHEN D.PRINCIPAL = A.EMPLOYEEID THEN 'Y' ELSE 'N' END AS 'Superior' ");
                    strSQL.Append(",g.name'EmployeeState', A.DomainServer'AD_Account', A.rank'Rank' ");
                    strSQL.Append("FROM Employee A LEFT JOIN JOB B ON A.JOBID = B.JOBID LEFT JOIN POSITION C ON B.POSITIONID = C.POSITIONID ");
                    strSQL.Append("LEFT JOIN DEPARTMENT D ON A.DEPARTMENTID = D.DEPARTMENTID LEFT JOIN CODEINFO E ON A.GENDERID = E.CodeInfoId ");
                    strSQL.Append("LEFT JOIN DISTRICT F ON A.AREAID = F.DISTRICTID LEFT JOIN[EmployeeState] AS g ON A.[EmployeeStateId] = g.[EmployeeStateId] order by Account ");

                    listResult = cnn.Query<SW_HR_ACCOUNT>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得所有部門新資料
        /// </summary>
        public List<SW_HR_DEPT> GetSW_HR_DEPT()
        {
            using (var cnn = GetANDESConnection())
            {
                List<SW_HR_DEPT> listResult = new List<SW_HR_DEPT>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append("SELECT  A.CODE 'Code', A.NAME 'Name', B.CODE 'Parent', C.CODE 'Superior' ");
                    strSQL.Append("From  DEPARTMENT  A LEFT  JOIN  Department  B  ON  A. ParentId = B. DepartmentId LEFT  JOIN  EMPLOYEE  C  ON  A.PRINCIPAL = C.EMPLOYEEID ");
                    strSQL.Append("Where A.Flag <> 0 ");

                    listResult = cnn.Query<SW_HR_DEPT>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得所有職稱新資料
        /// </summary>
        public List<SW_HR_LEVEL> GetSW_HR_LEVEL()
        {
            using (var cnn = GetANDESConnection())
            {
                List<SW_HR_LEVEL> listResult = new List<SW_HR_LEVEL>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append("SELECT  A.CODE 'Code', A.NAME 'Name' ");
                    strSQL.Append("FROM  JOB  A ");

                    listResult = cnn.Query<SW_HR_LEVEL>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得所有職級新資料
        /// </summary>
        public List<SW_HR_RANK> GetSW_HR_RANK()
        {
            using (var cnn = GetANDESConnection())
            {
                List<SW_HR_RANK> listResult = new List<SW_HR_RANK>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    strSQL.Append("SELECT DISTINCT A.RANK, 20 'Level' ");
                    strSQL.Append("FROM Employee A WHERE A.RANK IS NOT NULL ");

                    listResult = cnn.Query<SW_HR_RANK>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 取得所有兼任新資料
        /// </summary>
        public List<SW_HR_CONCURREN> GetSW_HR_CONCURREN()
        {
            using (var cnn = GetANDESConnection())
            {
                List<SW_HR_CONCURREN> listResult = new List<SW_HR_CONCURREN>();
                try
                {
                    cnn.Open();

                    StringBuilder strSQL = new StringBuilder();
                    //strSQL.Append(" SELECT Account, PartJobDepName, PartJobJobName, BeginDat, EndDate, (case when IsEffective = 'TRUE' then 1 else 0 end) as IsEffective ");
                    //strSQL.Append(" FROM CON ");
                    strSQL.Append("SELECT A.Code 'Account',B.Code 'PartJobDepCode',B.Name 'PartJobDepName',A.Rank 'PartJobJobName',EmployeePartJob.BeginDate 'BeginDate',EmployeePartJob.EndDate 'EndDate', (case when EmployeePartJob.IsEffective = 'True' then 1 else 0 end) as IsEffective ");
                    strSQL.Append("FROM EmployeePartJob AS EmployeePartJob LEFT  JOIN Employee AS A ON EmployeePartJob.EmployeeId = A.EmployeeId LEFT  JOIN Department AS B ON EmployeePartJob.DepartmentId = B.DepartmentId ");
                    strSQL.Append("LEFT  JOIN Job AS C ON EmployeePartJob.JobId = C.JobId ORDER BY EmployeePartJob.EmployeePartJobId ");

                    listResult = cnn.Query<SW_HR_CONCURREN>(strSQL.ToString()).ToList();
                }
                catch
                {
                    throw;
                }
                return listResult;
            }
        }

        /// <summary>
        /// 更新所有員工新資料
        /// </summary>
        public void updateSW_HR_ACCOUNT()
        {
            List<SW_HR_ACCOUNT> Account = GetSW_HR_ACCOUNT();
            //Logger.Write("ASDES", Account.ToString());
            using (var conn = GetConnection())
            {
                conn.Open();
                var tx = conn.BeginTransaction();
                try
                {
                    conn.Execute("DELETE FROM SW_HR_ACCOUNT", transaction: tx);
                    //Logger.Write("ASDES", "刪除ACCOUNT");
                    conn.Insert(Account, transaction: tx);
                    //Logger.Write("ASDES", "新增ACCOUNT");
                    tx.Commit();
                }
                catch (Exception ex)
                {
                    RollbackTransaction();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// 更新所有部門新資料
        /// </summary>
        public void updateSW_HR_DEPT()
        {
            List<SW_HR_DEPT> Dept = GetSW_HR_DEPT();
            using (var conn = GetConnection())
            {
                conn.Open();
                var tx = conn.BeginTransaction();
                try
                {
                    conn.Execute("DELETE FROM SW_HR_DEPT", transaction: tx);
                    conn.Insert(Dept, transaction: tx);
                    tx.Commit();
                }
                catch (Exception ex)
                {
                    RollbackTransaction();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// 更新所有職務新資料
        /// </summary>
        public void updateSW_HR_LEVEL()
        {
            List<SW_HR_LEVEL> Level = GetSW_HR_LEVEL();
            using (var conn = GetConnection())
            {
                conn.Open();
                var tx = conn.BeginTransaction();
                try
                {
                    conn.Execute("DELETE FROM SW_HR_LEVEL", transaction: tx);
                    conn.Insert(Level, transaction: tx);
                    tx.Commit();
                }
                catch (Exception ex)
                {
                    RollbackTransaction();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// 更新所有職級新資料
        /// </summary>
        public void updateSW_HR_RANK()
        {
            List<SW_HR_RANK> Rank = GetSW_HR_RANK();
            using (var conn = GetConnection())
            {
                conn.Open();
                var tx = conn.BeginTransaction();
                try
                {
                    conn.Execute("DELETE FROM SW_HR_RANK", transaction: tx);
                    conn.Insert(Rank, transaction: tx);
                    tx.Commit();
                }
                catch (Exception ex)
                {
                    RollbackTransaction();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// 更新所有兼任新資料
        /// </summary>
        public void updateSW_HR_CONCURREN()
        {
            List<SW_HR_CONCURREN> Concurren = GetSW_HR_CONCURREN();
            using (var conn = GetConnection())
            {
                conn.Open();
                var tx = conn.BeginTransaction();
                try
                {
                    conn.Execute("DELETE FROM SW_HR_CONCURREN", transaction: tx);
                    conn.Insert(Concurren, transaction: tx);
                    tx.Commit();
                }
                catch (Exception ex)
                {
                    RollbackTransaction();
                    throw ex;
                }
            }
        }*/

        public static SqlConnection GetConnection()
        {
            string dbConnStr = ConfigurationManager.ConnectionStrings["connectionstring"].ConnectionString;

            return new SqlConnection(dbConnStr);
        }

        public static SqlConnection GetAConnection()
        {
            string dbConnStr = ConfigurationManager.ConnectionStrings["connA"].ConnectionString;

            return new SqlConnection(dbConnStr);
        }

    }
}
