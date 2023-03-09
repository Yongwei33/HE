using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ede.Uof.EIP.Organization;
using Ede.Uof.EIP.Organization.Util;
using Ede.Uof.Utility.Configuration;
using Ede.Uof.Utility.Log;
using System.Configuration;
using System.Globalization;
using static Ede.Uof.EIP.Organization.DepartmentDataSet;
using Lib.Sync.Model;
using System.Data;
using System.Collections;
using Ede.Uof.Utility.Message;
using Ede.Uof.EIP.PrivateMessage;

namespace Lib.Sync.Sync
{
    public class SyncUCO
    {
        public string SyncLog
        {
            get
            {
                return messageList.ToString();
            }
        }

        private StringBuilder messageList = new StringBuilder();

        public void ClearAllCache()
        {
            Setting setting = new Setting();
            Group.Group group = new Group.Group();
            group.Url = setting["SiteUrl"] + "/WebService/Group.asmx";
            group.ClearAllCache();
        }

        public void RunSyncTask(string creator)
        {
            //部門
            List<SW_HR_DEPT> lst_DEPT;
            //員工
            List<SW_HR_ACCOUNT> lst_ACCOUNT;
            //職級
            List<SW_HR_JOBTITLE> lst_TITLE;
            //職務
            List<SW_HR_FUNCTION> lst_POSITION;

            SyncPO po = new SyncPO();

            try
            {
                //更新中介表
                /*AddLog("更新中介表");
                po.updateSW_HR_DEPT();
                po.updateSW_HR_LEVEL();
                po.updateSW_HR_RANK();
                po.updateSW_HR_ACCOUNT();
                po.updateSW_HR_CONCURREN();*/
                //同步部門
                lst_DEPT = po.Get_DEPT();

                if (lst_DEPT != null && lst_DEPT.Count != 0)
                {
                    AddLog($"取得部門資料 {lst_DEPT.Count} 筆");

                    SyncGroup(lst_DEPT);
                }
                else
                {
                    SendEmailNow("取得部門資料異常");
                    throw new Exception("取得部門資料異常");
                }

                //同步職級
                lst_TITLE = po.Get_TITLE();

                if (lst_TITLE != null && lst_TITLE.Count != 0)
                {
                    AddLog($"取得職級資料{lst_TITLE.Count}筆");

                    SyncJobTitle(lst_TITLE);
                }
                else
                {
                    SendEmailNow("取得職級資料異常");
                    throw new Exception("取得職級資料異常");
                }

                //同步職務
                /*lst_POSITION = po.Get_POSITON();

                
                AddLog($"取得職務資料 {lst_POSITION.Count} 筆");

                SyncJobFunc(lst_POSITION);*/

                //同步員工資訊
                lst_ACCOUNT = po.Get_ACCOUNT();

                if (lst_ACCOUNT != null && lst_ACCOUNT.Count != 0)
                {
                    AddLog($"取得員工資料{lst_ACCOUNT.Count}筆");

                    SyncUser(lst_ACCOUNT, lst_DEPT, lst_TITLE);
                }
                else
                {
                    SendEmailNow("取得員工資料異常");
                    throw new Exception("取得員工資料異常");
                }

                //設定員工兼任部門及設定部門主管(包含主要及兼任)
                List<SW_HR_CONCURREN> lstVM_Dept = po.GetCONCURREN(DateTime.Now.ToString("yyyy-MM-dd"));
                List<TB_EB_JOB_TITLE> lstUOFJobTitle = po.GetJOB_TITLE();

                if (lstUOFJobTitle != null && lstUOFJobTitle.Count != 0)
                {
                    SetEMPL_DEP_JobFUNC(lstVM_Dept, lstUOFJobTitle);
                }
                else
                {
                    SendEmailNow("取得員工兼任部門資料異常");
                    throw new Exception("取得員工兼任部門資料異常");
                }

                //比對停用員工
                CheckDisabledEMP(lst_ACCOUNT);

                AddLog("同步作業結束");
                Logger.Write("Lib_Sync", SyncLog);
            }
            catch (Exception ex)
            {
                AddLog($"{ex}");
                SendEmailNow($"{ex}");
                Logger.Write("Lib_Sync", SyncLog);
            }
            finally
            {
                ClearAllCache();
            }
        }

        // 同步部門
        private void SyncGroup(List<SW_HR_DEPT> srcLstDEPT)
        {
            string GroupId = string.Empty;
            string GroupName = string.Empty;
            string PreGroupName = string.Empty;
            string ParentGroupId = string.Empty;
            string GroupCode = string.Empty;

            string firstChar = string.Empty;
            int sync_Dept = 0;
            int failSync_Dept = 0;
            int update_Dept = 0;

            List<TB_EB_GROUP> lstUofGroup = new List<TB_EB_GROUP>();
            SyncPO syncPo = new SyncPO();

            try
            {
                //取得UOF部門最新資料
                DepartmentDataSet uofDeptDs = new DepartmentDataSet();
                GroupUCO groupUco = new GroupUCO(GroupType.Department);
                uofDeptDs = groupUco.QueryDepartment();

                List<SW_HR_DEPT> lstTempDeptList = new List<SW_HR_DEPT>();
                string upGroupName = string.Empty;
                string upGroupId = string.Empty;

                var item = uofDeptDs.Department.FirstOrDefault();
                if (item == null)
                {
                    GroupId = Guid.NewGuid().ToString();
                    GroupName = "和大工業";
                    ParentGroupId = "Company";
                    GroupCode = "和大工業";

                    groupUco.Create(GroupId, GroupName, ParentGroupId, GroupCode);
                }

                AddLog("開始同步部門");
                //依照部門代碼字母排序同步部門

                try
                {
                    GroupId = string.Empty;
                    GroupName = string.Empty;
                    PreGroupName = string.Empty;
                    ParentGroupId = string.Empty;
                    GroupCode = string.Empty;
                    lstTempDeptList.Clear();
                    string PId = string.Empty;

                    lstTempDeptList = srcLstDEPT.ToList();

                    foreach (SW_HR_DEPT obj in lstTempDeptList) //obj是最新的同步資料 第一個迴圈新增不在UOF的部門 parent=null
                    {
                        //檢查有無相同名稱和父部門的部門 有的話跳過不新增
                        List<SW_HR_DEPT> lstGroupName = lstTempDeptList.Where(t => t.Name == obj.Name).ToList();
                        if(lstGroupName.Count > 1)
                        {
                            List<SW_HR_DEPT> lstParent = lstGroupName.Where(t => t.Parent == obj.Parent).ToList();
                            if (lstParent.Count > 1)
                            {
                                SW_HR_DEPT ParentGroup = lstParent.Where(l => l.Parent == obj.Parent).FirstOrDefault();
                                string ParentGroupName = string.Empty;
                                if (!string.IsNullOrEmpty(ParentGroup.Parent))
                                    ParentGroupName = lstTempDeptList.Where(t => t.Code == ParentGroup.Parent).FirstOrDefault().Name;
                                else
                                    ParentGroupName = "和大工業";
                                AddLog($"{ ParentGroupName } 底下已存在 {obj.Name}");
                                SendEmailNow($"{ ParentGroup.Name } 底下已存在 {obj.Name}");
                                continue;
                            }
                        }
                        //撈取UOF資料庫內該部門資料
                        var UofDeptData = uofDeptDs.Department.Where(d => d.GROUP_CODE == obj.Code).FirstOrDefault();
                        //不存在則新增
                        if (UofDeptData == null)
                        {
                            GroupId = Guid.NewGuid().ToString();
                            GroupName = obj.Name;

                            if (lstGroupName.Count > 1 && !string.IsNullOrEmpty(PId))
                            {
                                ParentGroupId = PId;
                            }
                            else
                            {
                                ParentGroupId = "Company";
                            }
                            PId = GroupId;
                            GroupCode = obj.Code;

                            groupUco.Create(GroupId, GroupName, ParentGroupId, GroupCode);
                            //同步部門數
                            sync_Dept++;

                            //取得UOF部門最新資料
                            uofDeptDs.Clear();
                            uofDeptDs = groupUco.QueryDepartment();
                        }
                        else
                        {
                            PId = UofDeptData.GROUP_ID;
                        }
                    }
                    
                    foreach (SW_HR_DEPT obj in lstTempDeptList) //obj是最新的同步資料 第二個迴圈做更新和調整父部門
                    {
                        //撈取UOF資料庫內該部門資料
                        var UofDeptData = uofDeptDs.Department.Where(d => d.GROUP_CODE == obj.Code).FirstOrDefault();
                        if (UofDeptData != null)
                        {
                            GroupId = UofDeptData.GROUP_ID;
                            //撈取UOF部門資料
                            lstUofGroup.Clear();
                            lstUofGroup = syncPo.GetGROUP();

                            if (!string.IsNullOrEmpty(obj.Parent))
                            {
                                //找出該部門在UOF內的的父部門
                                var itemUpGroup = lstUofGroup.Where(g => g.GROUP_CODE == obj.Parent).FirstOrDefault();
                                if (itemUpGroup != null)
                                {
                                    upGroupId = itemUpGroup.GROUP_ID;
                                    upGroupName = itemUpGroup.GROUP_NAME;
                                    //比對UOF內該部門的PARENT_GROUP_ID跟當前的PARENT_GROUP_ID是否一致
                                    if (UofDeptData.PARENT_GROUP_ID != upGroupId)
                                    {
                                        groupUco.ChangeParent(GroupId, upGroupId);
                                        AddLog($"{UofDeptData.GROUP_NAME} 位置調整至 {upGroupName} | {upGroupId}底下");
                                        update_Dept++;
                                    }
                                }
                                else
                                {
                                    SendEmailNow($"{ obj.Code } 找不到上層部門資料");
                                    throw new Exception($"{ obj.Code } 找不到上層部門資料");
                                }
                            }
                            //找出該部門當前的名稱
                            var itemGroup = lstUofGroup.Where(g => g.GROUP_CODE == obj.Code).FirstOrDefault();
                            if (itemGroup != null)
                            {
                                //從UOF內該部門名稱
                                PreGroupName = itemGroup.GROUP_NAME;
                                //比對UOF內該部門名稱是否與當前的部門名稱相同
                                if (PreGroupName != obj.Name)
                                {
                                    groupUco.ChangeName(GroupId, obj.Name);
                                    AddLog($"{PreGroupName} 更換名稱為 {obj.Name}");
                                    update_Dept++;
                                }
                            }
                            else
                            {
                                SendEmailNow($"{ obj.Code } 找不到部門資料");
                                throw new Exception($"{ obj.Code } 找不到部門資料");
                            }
                        }
                    }
                }
                catch (GroupNameExistException ex)
                {
                    //同一層級中，群組名稱已存在
                    AddLog($"同一層級中，群組名稱已存在 {ex.Message}");
                    SendEmailNow($"同一層級中，群組名稱已存在 {ex.Message}");
                    failSync_Dept++;
                }
                catch (DepartmentNameExistException ex)
                {
                    //同一層級中，部門名稱已存在
                    AddLog($"同一層級中，部門名稱已存在 {ex.Message}");
                    SendEmailNow($"同一層級中，部門名稱已存在 {ex.Message}");
                    failSync_Dept++;
                }
                catch (Exception ex)
                {
                    AddLog($"同步部門作業異常，內容：{ex}");
                    SendEmailNow($"同步部門作業異常，內容：{ex}");
                    failSync_Dept++;
                }
                
                AddLog($"同步部門結束");
                AddLog($"新增 {sync_Dept} 個部門，更新 {update_Dept} 個部門");
                AddLog($"作業異常 {failSync_Dept} 個部門");

                uofDeptDs.Clear();
                uofDeptDs = groupUco.QueryDepartment();
            }
            catch (Exception ex)
            {
                SendEmailNow($"{ex}");
                AddLog($"{ex}");
            }
            finally
            {
                ClearAllCache();
            }
        }

        // 同步職級
        private void SyncJobTitle(List<SW_HR_JOBTITLE> srcLstRank)
        {
            //撈取UOF現有職級資料
            TitleUCO titleUCO = new TitleUCO();
            TitleDataSet uofTitleDs = titleUCO.Query();
            int syncTitle = 0;
            int existTitle = 0;

            AddLog("開始同步職級作業");
            foreach (SW_HR_JOBTITLE obj in srcLstRank)
            {
                try
                {
                    var itemTitle = uofTitleDs.Title.Where(t => t.TITLE_NAME == obj.Name).FirstOrDefault();
                    if (itemTitle == null)
                    {
                        string newTitleId = Guid.NewGuid().ToString();
                        titleUCO.CreateTitle(newTitleId, 30, obj.Name);
                        syncTitle++;
                    }
                    else
                    {
                        existTitle++;
                    }
                }
                catch (TitleNameExistException ex)
                {
                    SendEmailNow($"職級 {obj.Name} 已存在，{ex}");
                    AddLog($"職級 {obj.Name} 已存在，{ex}");
                }
                catch (Exception ex)
                {
                    SendEmailNow($"{ex}");
                    AddLog($"{ex}");
                }
            }

            AddLog($"同步職級作業結束");
            AddLog($"共新增 {syncTitle} 筆，已存在職級 {existTitle} 筆");

            ClearAllCache();
        }

        /// <summary>
        /// 同步職務
        /// </summary>
        /// <param name="lstSTDADM_POSITION"></param>
        /// <param name="srcName"></param>
        private void SyncJobFunc(List<SW_HR_FUNCTION> lst_POSITION)
        {
            FunctionUCO funcUCO = new FunctionUCO();
            FunctionDataSet funcUCOds = funcUCO.Query();
            int syncFunc = 0;
            int existFunc = 0;

            AddLog("開始同步職務");
            foreach (SW_HR_FUNCTION obj in lst_POSITION)
            {
                try
                {
                    //比對同步來的職務是否已存在UOF
                    var itemFunc = funcUCOds.Function.Where(f => f.FUNC_NAME == obj.Name).FirstOrDefault();
                    if (itemFunc == null)
                    {
                        //不存在則新增
                        string newFuncId = Guid.NewGuid().ToString();
                        funcUCO.CreateFunction(newFuncId, obj.Name);
                        syncFunc++;
                    }
                    else
                    {
                        //存在則記錄筆數+1
                        existFunc++;
                    }
                }
                catch (FunctionNameExistException ex)
                {
                    AddLog($"職務 {obj.Code} | {obj.Name}，{ex}");
                }
                catch (Exception ex)
                {
                    AddLog($"{ex}");
                }

            }

            funcUCOds = funcUCO.Query();

            AddLog($"同步職務結束");
            AddLog($"共新增 {syncFunc} 筆職務，已存在職務 {existFunc} 筆");

            ClearAllCache();

        }

        // 同步員工資訊
        private void SyncUser(List<SW_HR_ACCOUNT> srcLst_EMP, List<SW_HR_DEPT> srcLst_Dept, List<SW_HR_JOBTITLE> srcLst_Rank)
        {
            try
            {
                SyncPO syncPO = new SyncPO();
                UserUCO userUCO = new UserUCO();
                int createCount = 0;
                int modifyCount = 0;

                // 重戴一次部門取得最新資料
                GroupUCO groupUco = new GroupUCO(GroupType.Department);
                DepartmentDataSet uofDeptDs = groupUco.QueryDepartment();

                //撈取UOF職級資料
                TitleUCO titleUCO = new TitleUCO();
                TitleDataSet uofJobTitleDs = titleUCO.Query();
                List<TB_EB_JOB_TITLE> lstJobTitle = syncPO.GetJOB_TITLE();

                //撈取UOF職務資料
                FunctionUCO funcUCO = new FunctionUCO();
                FunctionDataSet uofFuncDs = funcUCO.Query();

                //同步員工
                foreach (SW_HR_ACCOUNT emp in srcLst_EMP)
                {
                    try
                    {
                        //取得員工GUID
                        string userGuid = userUCO.GetGUID(emp.Account.Trim());

                        if (string.IsNullOrEmpty(userGuid))
                        {
                            //AddLog($"開始新增員工");
                            CreateUser(emp, uofDeptDs, uofFuncDs, lstJobTitle, srcLst_Rank);
                            createCount++;
                        }
                        else
                        {
                            //取得員工的emp資料
                            EmployeeUCO empUCO = new EmployeeUCO();
                            EmployeeDataSet empDs = new EmployeeDataSet();
                            empDs = empUCO.GetEmployeeByUserGuid(userGuid);
                            //AddLog($"開始新增更新員工");
                            ModifyUser(userGuid, empDs, emp, uofDeptDs, uofFuncDs, srcLst_Rank, lstJobTitle);
                            modifyCount++;
                        }
                    }
                    catch (Exception ex)
                    {
                        SendEmailNow($"同步員工作業異常，內容：{ex}，員工工號：{emp.Account} | 姓名：{emp.Name} | 部門：{emp.Department} ");
                        AddLog($"同步員工作業異常，員工工號：{emp.Account} | 姓名：{emp.Name} | 部門：{emp.Department} ");
                        AddLog($"同步員工作業異常，內容：{ex}");
                    }
                }
                AddLog($"同步員工作業結束");
                AddLog($"新增 {createCount} 位員工、更新 {modifyCount} 位員工 ");
            }
            catch (Exception ex)
            {
                SendEmailNow($"同步員工作業異常，內容：{ex}");
                AddLog($"同步員工作業異常，內容：{ex}");
            }
            finally
            {
                ClearAllCache();
            }
        }

        // 建立員工
        private void CreateUser(SW_HR_ACCOUNT emp, DepartmentDataSet uofDeptDs, FunctionDataSet uofFuncDs, List<TB_EB_JOB_TITLE> srcLstJobTitle, List<SW_HR_JOBTITLE> srcLst_Rank)
        {
            SyncPO po = new SyncPO();

            try
            {
                EmployeeDataSet empDs = new EmployeeDataSet();
                EmployeeDataSet.EmployeeRow uofUser = empDs.Employee.NewEmployeeRow();

                //GIOD
                uofUser.USER_GUID = Guid.NewGuid().ToString();

                uofUser.ACCOUNT = emp.Account.Trim();
                uofUser.NAME = emp.Name;
                uofUser.IS_LOCKED_OUT = false;
                uofUser.IS_SUSPENDED = false;
                uofUser.LAST_SUSPENDED_DATE = DateTime.Now;
                uofUser.IS_AD_AUTH = false;
                uofUser.CREATE_DATE = DateTime.Now; //CREATE_DATE不可為null
                uofUser.LANG = string.Empty; //語系不可為null

                //帳號到期日
                uofUser.EXPIRE_DATE = DateTime.Parse("9999-12-31 23:59:59.997"); //到期日
                //到職日
                if (!string.IsNullOrEmpty(emp.ArriveDay))
                {
                    DateTime deCBEG_DATE = DateTime.Parse(emp.ArriveDay);
                    //DateTime dtARRIVE_DATE = DateTime.ParseExact(deCBEG_DATE.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
                    uofUser.ARRIVE_DATE = deCBEG_DATE;
                }
                else
                    uofUser.SetARRIVE_DATENull();
                //Email
                /*if (!string.IsNullOrEmpty(emp.Email))
                    uofUser.EMAIL = emp.Email;
                else
                    uofUser.SetEMAILNull();*/
                //地址
                if (!string.IsNullOrEmpty(emp.Location))
                    uofUser.ADDRESS = emp.Location;
                else
                    uofUser.SetADDRESSNull();
                //生日
                if (!string.IsNullOrEmpty(emp.BirthDay))
                {
                    DateTime deBirth = DateTime.Parse(emp.BirthDay);
                    //var dtBirth = DateTime.ParseExact(deBirth.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
                    uofUser.BIRTHDAY = deBirth;
                }
                else
                    uofUser.SetBIRTHDAYNull();
                //性別
                if (!string.IsNullOrEmpty(emp.Sex))
                {
                    if (emp.Sex.Equals("M"))
                        uofUser.SEX = "M";
                    else
                        uofUser.SEX = "F";
                }
                else
                    uofUser.SetSEXNull();
                //AD帳號
                if (!string.IsNullOrEmpty(emp.AD_Account))
                    uofUser.ACCOUNT_MAPPING = emp.AD_Account;
                //時區
                if (!string.IsNullOrEmpty(emp.TimeZone))
                    uofUser.DISPLAY_TIMEZONE = emp.TimeZone;

                // 1.新增員工
                empDs.Employee.AddEmployeeRow(uofUser);

                // 2.新增的員工掛入主要部門之下     
                EmployeeDataSet.DepartmentRow drDept = empDs.Department.NewDepartmentRow();
                drDept.USER_GUID = uofUser.USER_GUID;
                drDept.ORDERS = 0;

                IEnumerable<DepartmentRow> rowGroup = uofDeptDs.Department.Where(d => d.GROUP_CODE == emp.Dept_Code);
                if (rowGroup.Count() == 0)
                {
                    AddLog($"找不到員工 {emp.Name} | {emp.Account} 所屬部門 {emp.Department}");
                    SendEmailNow($"找不到員工 {emp.Name} | {emp.Account} 所屬部門 {emp.Department}");
                    throw new Exception($"找不到員工 {emp.Name} | {emp.Account} 所屬部門 {emp.Department}");
                }
                else
                {
                    drDept.GROUP_ID = rowGroup.First().GROUP_ID;
                }

                // 3.新增員工所屬的職級(找出職稱名稱後在比對是否存在UOF中)
                if (emp.TitleName == "一般員工")
                {
                    string titleName = string.Empty;
                    string titleId = string.Empty;
                    //從職級清單中找出所屬職級名稱
                    var itemTitleName = srcLst_Rank.Where(t => t.Name == "無(一般員工)").FirstOrDefault();
                    if (itemTitleName != null)
                    {
                        titleName = itemTitleName.Name;
                    }
                    else
                    {
                        SendEmailNow($"找不到職級代碼{emp.TitleName}所對應職級名稱");
                        throw new Exception($"找不到職級代碼{emp.TitleName}所對應職級名稱");
                    }
                    var itemTitleId = srcLstJobTitle.Where(t => t.TITLE_NAME == titleName).FirstOrDefault();
                    if (itemTitleId != null)
                    {
                        titleId = itemTitleId.TITLE_ID;
                    }
                    if (!string.IsNullOrEmpty(titleId))
                    {
                        drDept.TITLE_ID = titleId;
                    }
                    else
                    {
                        drDept.SetTITLE_IDNull();
                    }
                }
                else if (!string.IsNullOrEmpty(emp.TitleName) && emp.TitleName != "一般員工")
                {
                    string titleName = string.Empty;
                    string titleId = string.Empty;
                    //從職級清單中找出所屬職級名稱
                    var itemTitleName = srcLst_Rank.Where(t => t.Name == emp.TitleName).FirstOrDefault();
                    if (itemTitleName != null)
                    {
                        titleName = itemTitleName.Name;
                    }
                    else
                    {
                        SendEmailNow($"找不到職級代碼{emp.TitleName}所對應職級名稱");
                        throw new Exception($"找不到職級代碼{emp.TitleName}所對應職級名稱");
                    }
                    var itemTitleId = srcLstJobTitle.Where(t => t.TITLE_NAME == titleName).FirstOrDefault();
                    if (itemTitleId != null)
                    {
                        titleId = itemTitleId.TITLE_ID;
                    }
                    if (!string.IsNullOrEmpty(titleId))
                    {
                        drDept.TITLE_ID = titleId;
                    }
                    else
                    {
                        drDept.SetTITLE_IDNull();
                    }
                }
                else
                {
                    drDept.SetTITLE_IDNull();
                }

                empDs.Department.AddDepartmentRow(drDept);

                // 4職務
                /*EmployeeDataSet.FunctionRow drFunc = empDs.Function.NewFunctionRow();
                drFunc.USER_GUID = uofUser.USER_GUID;
                drFunc.GROUP_ID = rowGroup.First().GROUP_ID;
                if (!string.IsNullOrEmpty(emp.FunctionName))
                {
                    var itemFunc1 = uofFuncDs.Function.Where(f => f.FUNC_NAME == emp.FunctionName).FirstOrDefault();
                    if (itemFunc1 != null)
                    {
                        drFunc.FUNC_ID = itemFunc1.FUNC_ID;
                    }
                    else
                    {
                        throw new Exception("找不到職務代碼");
                    }
                    empDs.Function.AddFunctionRow(drFunc);
                }*/

                EmployeeUCO empUCO = new EmployeeUCO();
                empUCO.Create(empDs);
            }
            catch (Exception ex)
            {
                SendEmailNow($"{ex}");
                throw;
            }
        }

        // 編輯員工
        private void ModifyUser(string oldUserGuid, EmployeeDataSet empDs, SW_HR_ACCOUNT emp, DepartmentDataSet uofDeptDs, FunctionDataSet uofFuncDs, List<SW_HR_JOBTITLE> srcLstRank, List<TB_EB_JOB_TITLE> srcLstJobTitle)
        {
            string titleName = string.Empty;
            string titleId = string.Empty;
            string funcName = string.Empty;
            string funcId = string.Empty;
            string curSyncGroupId = string.Empty;
            string curSyncGroupName = string.Empty;
            string preSyncGroupId = string.Empty;
            string preSyncGroupName = string.Empty;
            try
            {
                EmployeeDataSet.EmployeeRow oldUofUserRow = empDs.Employee.FindByUSER_GUID(oldUserGuid);
                SyncPO syncPO = new SyncPO();

                //取得員工前次同步在UOF內的部門編號
                try
                {
                    List<TB_EB_EMPL_DEP> lstEMPL_DEP = syncPO.GetEMPL_DEP(oldUserGuid);
                    if (lstEMPL_DEP.Count != 0)
                    {
                        preSyncGroupId = lstEMPL_DEP.Where(d => d.ORDERS == 0).FirstOrDefault().GROUP_ID;
                        preSyncGroupName = uofDeptDs.Department.Where(d => d.GROUP_ID == preSyncGroupId).FirstOrDefault().GROUP_NAME;
                    }
                }
                catch (Exception ex)
                {
                    SendEmailNow($"取得人員：{emp.Name}，在UOF部門資料時發生錯誤，{ex.Message}");
                    throw new Exception($"取得人員：{emp.Name}，在UOF部門資料時發生錯誤，{ex.Message}");
                }

                //取得員工職務ID
                /*if (!string.IsNullOrEmpty(emp.FunctionName))
                {
                    try
                    {
                        funcId = uofFuncDs.Function.Where(f => f.FUNC_NAME == emp.FunctionName).FirstOrDefault().FUNC_ID;
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"人員：{emp.FunctionName}，取得職務資料時發生錯誤，{ex.Message}");
                    }
                }*/
                
                //取得員工此次同步所屬部門在UOF內的GroupID
                try
                {
                    curSyncGroupId = uofDeptDs.Department.Where(d => d.GROUP_CODE == emp.Dept_Code).FirstOrDefault().GROUP_ID;
                    curSyncGroupName = uofDeptDs.Department.Where(d => d.GROUP_ID == curSyncGroupId).FirstOrDefault().GROUP_NAME;

                }
                catch (Exception ex)
                {
                    SendEmailNow($"人員：{emp.Name}，取得部門ID時發生錯誤，{ex.Message}");
                    throw new Exception($"人員：{emp.Name}，取得部門ID時發生錯誤，{ex.Message}");
                }

                try
                {
                    if (emp.TitleName == "一般員工")
                    {
                        titleName = srcLstRank.Where(t => t.Name == "無(一般員工)").FirstOrDefault().Name;
                        titleId = srcLstJobTitle.Where(t => t.TITLE_NAME == titleName).FirstOrDefault().TITLE_ID;
                    }
                    else if (!string.IsNullOrEmpty(emp.TitleName) && emp.TitleName != "一般員工")
                    {
                        //取得員工此次同步所屬職級名稱
                        titleName = srcLstRank.Where(t => t.Name == emp.TitleName).FirstOrDefault().Name;
                        titleId = srcLstJobTitle.Where(t => t.TITLE_NAME == titleName).FirstOrDefault().TITLE_ID;
                    }
                    else
                    {
                        titleId = string.Empty;
                    }
                }
                catch (Exception ex)
                {
                    SendEmailNow($"{ emp.Account } 取得職級ID時發生錯誤");
                    throw new Exception($"{ emp.Account } 取得職級ID時發生錯誤");
                }

                oldUofUserRow.USER_GUID = oldUserGuid;
                oldUofUserRow.ACCOUNT = emp.Account.Trim();
                oldUofUserRow.NAME = emp.Name;

                oldUofUserRow.IS_LOCKED_OUT = false;
                //如果員工為停用後再次啟用(STATE由N改為Y)
                if (oldUofUserRow.IS_SUSPENDED)
                {
                    //設定員工帳號啟用日期
                    oldUofUserRow.LAST_ACTIVITY_DATE = DateTime.Now;
                }
                else
                    oldUofUserRow.SetLAST_ACTIVITY_DATENull();

                oldUofUserRow.IS_SUSPENDED = false;
                oldUofUserRow.LAST_SUSPENDED_DATE = DateTime.Now;
                oldUofUserRow.IS_AD_AUTH = false;
                oldUofUserRow.CREATE_DATE = DateTime.Now; //CREATE_DATE不可為null
                oldUofUserRow.LANG = string.Empty; //語系不可為null
                //帳號到期日
                oldUofUserRow.EXPIRE_DATE = DateTime.Parse("9999-12-31 23:59:59.997"); //永久有效
                //到職日
                if (!string.IsNullOrEmpty(emp.ArriveDay))
                {
                    DateTime deCBEG_DATE = DateTime.Parse(emp.ArriveDay);
                    //DateTime dtARRIVE_DATE = DateTime.ParseExact(deCBEG_DATE.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
                    oldUofUserRow.ARRIVE_DATE = deCBEG_DATE;
                }
                else
                    oldUofUserRow.SetARRIVE_DATENull();
                //Email
                /*if (!string.IsNullOrEmpty(emp.Email))
                    oldUofUserRow.EMAIL = emp.Email;
                else
                    oldUofUserRow.SetEMAILNull();*/
                //地址
                if (!string.IsNullOrEmpty(emp.Location))
                    oldUofUserRow.ADDRESS = emp.Location;
                else
                    oldUofUserRow.SetADDRESSNull();
                //生日
                if (!string.IsNullOrEmpty(emp.BirthDay))
                {
                    DateTime deBIRTH = DateTime.Parse(emp.BirthDay);
                    //DateTime dtBirth = DateTime.ParseExact(deBIRTH.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
                    oldUofUserRow.BIRTHDAY = deBIRTH;
                }
                else
                    oldUofUserRow.SetBIRTHDAYNull();
                //性別
                if (!string.IsNullOrEmpty(emp.Sex))
                {
                    if (emp.Sex.Equals("M"))
                        oldUofUserRow.SEX = "M";
                    else
                        oldUofUserRow.SEX = "F";
                }
                else
                    oldUofUserRow.SetSEXNull();
                //AD帳號
                if (!string.IsNullOrEmpty(emp.AD_Account))
                    oldUofUserRow.ACCOUNT_MAPPING = emp.AD_Account;
                //時區
                if (!string.IsNullOrEmpty(emp.TimeZone))
                    oldUofUserRow.DISPLAY_TIMEZONE = emp.TimeZone;

                //更新人員資訊
                EmployeeUCO empUCO = new EmployeeUCO();
                empUCO.Update(empDs);

                //比對所屬部門是否有異動
                if (curSyncGroupId != preSyncGroupId)
                {
                    //更新 新主要部門及所屬主要職級
                    //1.先刪除原部門及所屬職級
                    //2.新增新部門及所屬職級
                    syncPO.UpdateEmp_NewMain_Dept_Title(oldUserGuid, preSyncGroupId, curSyncGroupId, titleId);

                    /*if (!string.IsNullOrEmpty(emp.FunctionName))
                    {
                        //更新 新主要部門及所屬主要職務
                        //1.先刪除原部門的職務、包含員工、簽核人員及部門主管職務(若該員工為部門主管)
                        //2.新增新部門的主要職務
                        syncPO.UpdateEmp_New_Main_GroupFunc(oldUserGuid, preSyncGroupId, curSyncGroupId, funcId);
                    }*/

                    AddLog($"員工{emp.Name}，所屬部門由 {preSyncGroupName} 調至 {curSyncGroupName} ");
                }
                else if (curSyncGroupId == preSyncGroupId) //無部門異動
                {
                    //更新主要部門及所屬主要職級
                    //1.刪除主要部門及主要職級
                    //2.新增主要部門及主要職級
                    syncPO.UpdateEmp_Main_Dept_Title(oldUserGuid, preSyncGroupId, titleId);

                    /*if (!string.IsNullOrEmpty(emp.FunctionName))
                    {
                        //部門未調動，但員工主要職務異動，則依下列方式進行
                        //1.刪除所屬部門的主要職務
                        //2.新增所屬部門的主要職務。
                        syncPO.UpdateEmp_Main_GroupFunc(oldUserGuid, preSyncGroupId, funcId);
                    }*/
                }
            }
            catch (Exception ex)
            {
                SendEmailNow($"{ex}");
                throw;
            }
        }

        private void SetEMPL_DEP_JobFUNC(List<SW_HR_CONCURREN> srcLstVM_DEPT, List<TB_EB_JOB_TITLE> srcLstUOF_JobTitle)
        {
            try
            {
                AddLog($"開始設定兼任部門及所屬職級");
                //取得UOF組織
                DepartmentDataSet uofDeptDs = new DepartmentDataSet();
                GroupUCO groupUco = new GroupUCO(GroupType.Department);
                uofDeptDs = groupUco.QueryDepartment();

                //取得UOF職稱
                TitleUCO titleUco = new TitleUCO();
                TitleDataSet uofJobTitleDs = new TitleDataSet();
                uofJobTitleDs = titleUco.Query();

                //撈取UOF職務資料
                FunctionUCO funcUCO = new FunctionUCO();
                FunctionDataSet uofFuncDs = funcUCO.Query();

                //初始化
                List<SW_HR_CONCURREN> lstEmpDept = new List<SW_HR_CONCURREN>();
                List<string> lstEmpMainDept = new List<string>();
                SyncPO syncPo = new SyncPO();
                UserUCO usrUco = new UserUCO();
                string userGuid = string.Empty;
                string groupId = string.Empty;
                string titleName = string.Empty;
                string titleId = string.Empty;
                int orders = 1;

                //判斷原先於UOF內部門主管，是否與同步HR系統的部門主管相同
                string uofGroupCode = string.Empty;
                string uofGroupId = string.Empty;
                string uofGroupName = string.Empty;
                string uofSuperiorGuid = string.Empty;
                string uofSuperiorAccount = string.Empty;
                string uofSuperiorName = string.Empty;

                //取得所有部門主管工號
                List<string> lstMGREmpNo = srcLstVM_DEPT.Select(t => t.Account).Distinct().OrderBy(t => t).ToList();

                //刪除不在中介表的兼職職務
                List<TB_EB_EMPL_DEP> lstAllDep = syncPo.GetAllEMPL_DEP();
                foreach (TB_EB_EMPL_DEP dep in lstAllDep)
                {
                    string emp = usrUco.GetEBUser(dep.USER_GUID).Account;
                    List<SW_HR_CONCURREN> lstEmpNo = srcLstVM_DEPT.Where(t => t.Account == emp).OrderBy(t => t).ToList();
                    if(lstEmpNo.Count > 0)
                    {
                        string code = uofDeptDs.Department.Where(d => d.GROUP_ID == dep.GROUP_ID).FirstOrDefault().GROUP_CODE;
                        List<SW_HR_CONCURREN> lstGroupCode = lstEmpNo.Where(t => t.PartJobDepCode == code).OrderBy(t => t).ToList();
                        if (lstGroupCode.Count == 0)
                            syncPo.DeleteOneEmp_Dept(dep.USER_GUID, dep.GROUP_ID);
                    }
                    else
                    {
                        syncPo.DeleteOneEmp_Dept(dep.USER_GUID, dep.GROUP_ID);
                    }
                }

                //開始設定(((兼任部門)))及兼任部門的簽核人員職務
                foreach (string empNo in lstMGREmpNo)
                {
                    //設定ORDERS初始值
                    orders = 1;
                    titleName = string.Empty;
                    userGuid = string.Empty;
                    groupId = string.Empty;
                    titleId = string.Empty;

                    userGuid = usrUco.GetGUID(empNo);

                    //刪除主管"所有兼任部門"，不包含主要部門的簽核人員職務
                    List<TB_EB_EMPL_DEP> lstDep = syncPo.GetEMPL_DEP(userGuid);
                    TB_EB_EMPL_DEP dep = lstDep.Where(d => d.ORDERS == 0).FirstOrDefault();
                    syncPo.DeleteEmp_Dept(userGuid, dep.GROUP_ID);

                    //篩選出該工號所有部門
                    lstEmpDept.Clear();
                    lstEmpDept = srcLstVM_DEPT.Where(t => t.Account == empNo).ToList();

                    if (lstEmpDept.Count != 0)
                    {
                        foreach (var item in lstEmpDept)
                        {
                            groupId = string.Empty;
                            try
                            {
                                string Code = item.PartJobDepCode.Replace("\r", "");
                                string depCode = Code.Replace("\n", "");
                                var item1 = uofDeptDs.Department.Where(d => d.GROUP_CODE == depCode).FirstOrDefault();
                                if (item1 != null)
                                {
                                    groupId = item1.GROUP_ID;
                                }
                                else
                                {
                                    SendEmailNow($"{ item.PartJobDepCode } 設定主管兼任部門作業錯誤，內容：無法取得Group_ID)");
                                    throw new Exception($"{ item.PartJobDepCode } 設定主管兼任部門作業錯誤，內容：無法取得Group_ID)");
                                }

                                //設定兼任部門職稱
                                if (!string.IsNullOrEmpty(item.PartJobJobTitleName))
                                {
                                    var itemTitleId = srcLstUOF_JobTitle.Where(t => t.TITLE_NAME == item.PartJobJobTitleName).FirstOrDefault();
                                    if (itemTitleId != null)
                                        titleId = itemTitleId.TITLE_ID;
                                }

                                //新增兼任部門及簽核人員職務
                                //AddLog(item.PartJobFunctionName);
                                string itemFunction = "";
                                if (!string.IsNullOrEmpty(item.PartJobFunctionName))
                                    itemFunction = item.PartJobFunctionName;
                                try
                                {
                                    if (itemFunction.Contains("簽核人員"))
                                        syncPo.InsertEMP_PT_DEP_FUNC(groupId, userGuid, titleId, orders);
                                    else
                                        syncPo.InsertEMP_PT_DEP(groupId, userGuid, titleId, orders);
                                }
                                catch (Exception ex)
                                {
                                    AddLog($"{ex}");
                                }
                                    
                                    //新增簽核人員外的其他職務
                                    /*string[] functionArray = itemFunction.Split(',');
                                    foreach (var Array in functionArray)
                                    {
                                        string function = Array.Trim();
                                        //AddLog(function);
                                        if ( function != "簽核人員" && function != "部門主管")
                                        {
                                            var itemFunc = uofFuncDs.Function.Where(f => f.FUNC_NAME == function).FirstOrDefault();
                                            if (itemFunc != null)
                                            {
                                                syncPo.InsertEMP_PT_FUNC(groupId, userGuid, itemFunc.FUNC_ID);
                                            }
                                            else
                                            {
                                                AddLog("找不到職務代碼");
                                            }
                                        }
                                    }*/
                                orders++;
                            }
                            catch (Exception ex)
                            {
                                SendEmailNow($"{ex}");
                                AddLog($"{ex}");
                            }
                        }
                    }
                }

                List<SW_HR_DEPT> lstHRA_DEPT = new List<SW_HR_DEPT>();
                lstHRA_DEPT = syncPo.Get_DEPT();

                foreach (var syncDept in lstHRA_DEPT)
                {
                    //取得UOF_GroupCode
                    uofGroupCode = syncDept.Code;

                    //取得UOF_GroupId
                    var uofGroup = uofDeptDs.Department.Where(d => d.GROUP_CODE == uofGroupCode).FirstOrDefault();

                    if (uofGroup != null)
                    {
                        uofGroupId = uofGroup.GROUP_ID;
                        uofGroupName = uofGroup.GROUP_NAME;

                        //取得UOF部門主管工號
                        //為空值表示該部門於UOF目前無部門主管
                        List<TB_EB_EMPL_FUNC> lstGoupItems = new List<TB_EB_EMPL_FUNC>();
                        lstGoupItems = syncPo.GetUOF_Superior(uofGroupId);
                        if (lstGoupItems != null && lstGoupItems.Count != 0)
                        {
                            foreach (var group in lstGoupItems)
                            {
                                uofSuperiorGuid = group.USER_GUID;
                                uofSuperiorAccount = usrUco.GetEBUser(uofSuperiorGuid).Account;
                                uofSuperiorName = usrUco.GetEBUser(uofSuperiorGuid).Name;

                                if (!string.IsNullOrEmpty(syncDept.Superior))
                                {
                                    userGuid = usrUco.GetGUID(syncDept.Superior);

                                    //部門主管換人
                                    //比對部門主管是否相同
                                    if (uofSuperiorAccount != syncDept.Superior)
                                    {
                                        //判斷新主管和該部門在DEP裡有無資料，有則刪除舊主管，無則跳過不處理
                                        List<TB_EB_EMPL_DEP> listNewDepts = syncPo.GetEMPL_DEP(userGuid);
                                        var newDept = listNewDepts.Where(g => g.GROUP_ID == group.GROUP_ID).FirstOrDefault();
                                        if(newDept != null)
                                        {
                                            AddLog($"UOF {uofGroupName} 部門主管 {uofSuperiorAccount} / {uofSuperiorName} 與HR系統同步部門主管 {syncDept.Superior} 不同 ");

                                            //不同則刪除所在部門的部門主管及簽核人員職務
                                            //syncPo.DeleteEMPL_SUPERIOR_ByGROUP_ID(uofSuperiorGuid, uofGroupId);
                                            //AddLog($"刪除 {uofSuperiorAccount}/{uofSuperiorName} 於{uofGroupName} 部門主管及簽核人員職務");

                                            //判斷所在部門是否為主要部門
                                            List<TB_EB_EMPL_DEP> listAllDepts = syncPo.GetEMPL_DEP(uofSuperiorGuid);
                                            var itemDept = listAllDepts.Where(g => g.GROUP_ID == group.GROUP_ID).FirstOrDefault();
                                            if (itemDept != null)
                                            {
                                                bool bMainDept = itemDept.ORDERS == 0;

                                                if (!bMainDept)
                                                {
                                                    //非主要部門則刪除所在部門和部門主管及簽核人員職務
                                                    syncPo.DeleteEMPL_SUPERIOR_ByGROUP_ID(uofSuperiorGuid, uofGroupId, 1);
                                                    AddLog($"刪除 {uofSuperiorAccount}/{uofSuperiorName} 於{uofGroupName} 部門主管及簽核人員職務");
                                                    AddLog($"{uofGroupName} 非 {uofSuperiorAccount}/{uofSuperiorName} 主要部門，刪除所在部門");
                                                    syncPo.DeleteEMPL_DEPTByUserGuid_GROUP_ID(uofSuperiorGuid, uofGroupId);
                                                }
                                                else
                                                {
                                                    //刪除所在部門的部門主管
                                                    syncPo.DeleteEMPL_SUPERIOR_ByGROUP_ID(uofSuperiorGuid, uofGroupId, 0);
                                                    AddLog($"刪除 {uofSuperiorAccount}/{uofSuperiorName} 於{uofGroupName} 部門主管");
                                                }
                                            }
                                        }
                                        else
                                        {
                                            AddLog($"{uofGroupName} 新部門主管 {syncDept.Superior}/{usrUco.GetEBUser(userGuid).Name} 找不到兼任資料，跳過不處理");
                                            SendEmailNow($"{uofGroupName} 新部門主管 {syncDept.Superior}/{usrUco.GetEBUser(userGuid).Name} 找不到兼任資料，跳過不處理");
                                        }
                                    }
                                }
                                else //撤除部門主管
                                {
                                    //刪除所在部門的部門主管及簽核人員職務
                                    AddLog($"HR系統同步{uofGroupName} 未設定部門主管 ");
                                    //AddLog($"刪除 {uofSuperiorAccount}/{uofSuperiorName} 於{uofGroupName} 部門主管及簽核人員職務");
                                    //syncPo.DeleteEMPL_SUPERIOR_ByGROUP_ID(uofSuperiorGuid, uofGroupId, 0);

                                    //判斷所在部門是否為主要部門
                                    List<TB_EB_EMPL_DEP> listAllDepts = syncPo.GetEMPL_DEP(uofSuperiorGuid);
                                    var itemDept = listAllDepts.Where(g => g.GROUP_ID == group.GROUP_ID).FirstOrDefault();
                                    if (itemDept != null)
                                    {
                                        bool bMainDept = itemDept.ORDERS == 0;

                                        if (!bMainDept)
                                        {
                                            //非主要部門則刪除所在部門和部門主管及簽核人員職務
                                            syncPo.DeleteEMPL_SUPERIOR_ByGROUP_ID(uofSuperiorGuid, uofGroupId, 1);
                                            AddLog($"刪除 {uofSuperiorAccount}/{uofSuperiorName} 於{uofGroupName} 部門主管及簽核人員職務");
                                            AddLog($"{uofGroupName} 非 {uofSuperiorAccount}/{uofSuperiorName} 主要部門，刪除所在部門");
                                            syncPo.DeleteEMPL_DEPTByUserGuid_GROUP_ID(uofSuperiorGuid, uofGroupId);
                                        }
                                        else
                                        {
                                            //刪除所在部門的部門主管
                                            syncPo.DeleteEMPL_SUPERIOR_ByGROUP_ID(uofSuperiorGuid, uofGroupId, 0);
                                            AddLog($"刪除 {uofSuperiorAccount}/{uofSuperiorName} 於{uofGroupName} 部門主管");
                                        }
                                    }
                                }
                            }
                        }
                        //如果部門的主管不在職務表裡就設定部門主管
                        if (!string.IsNullOrEmpty(syncDept.Superior))
                        {
                            userGuid = usrUco.GetGUID(syncDept.Superior);
                            if (!string.IsNullOrEmpty(userGuid))
                            {
                                lstGoupItems = syncPo.GetUOF_SuperiorGUID(uofGroupId, userGuid);
                                if (lstGoupItems.Count == 0)
                                {
                                    List<TB_EB_EMPL_DEP> listNewDepts = syncPo.GetEMPL_DEP(userGuid);
                                    var newDept = listNewDepts.Where(g => g.GROUP_ID == uofGroupId).FirstOrDefault();
                                    if (newDept != null)
                                    {
                                        syncPo.InsertJobFunction(uofGroupId, userGuid);
                                        AddLog($"{uofGroupName} 部門主管設為 {syncDept.Superior}/{usrUco.GetEBUser(userGuid).Name} ");
                                        //若無簽核職務則賦予簽核職務
                                        var signer = syncPo.GetUOF_Signer(userGuid, uofGroupId);
                                        if (signer.Count == 0)
                                        {
                                            syncPo.InsertJobFunctionSigner(userGuid, uofGroupId);
                                        }
                                    }
                                    else
                                    {
                                        AddLog($"{uofGroupName} 部門主管 {syncDept.Superior}/{usrUco.GetEBUser(userGuid).Name} 找不到兼任資料");
                                        SendEmailNow($"{uofGroupName} 部門主管 {syncDept.Superior}/{usrUco.GetEBUser(userGuid).Name} 找不到兼任資料");
                                    }
                                }
                                else
                                {
                                    //若無簽核職務則賦予簽核職務
                                    var signer = syncPo.GetUOF_Signer(userGuid, uofGroupId);
                                    if (signer.Count == 0)
                                    {
                                        syncPo.InsertJobFunctionSigner(userGuid, uofGroupId);
                                    }
                                }
                            }
                            else
                            {
                                AddLog($"{uofGroupName} 部門找不到主管ID {syncDept.Superior} ");
                                SendEmailNow($"{uofGroupName} 部門找不到主管ID {syncDept.Superior} ");
                            }
                        }
                        else
                        {
                            AddLog($"{uofGroupName} 部門沒有設主管 {syncDept.Superior} ");
                            SendEmailNow($"{uofGroupName} 部門沒有設主管 {syncDept.Superior} ");
                        }
                    }
                    else
                    {
                        AddLog($"UOF找不到部門 {uofGroupCode} ");
                        SendEmailNow($"UOF找不到部門 {uofGroupCode} ");
                    }
                }

                AddLog($"設定兼任部門及所屬職級結束");
            }
            catch (Exception ex)
            {
                SendEmailNow($"{ex}");
                AddLog($"{ex}");
            }
            finally
            {
                ClearAllCache();
            }
        }

        /// <summary>
        /// 檢查停用員工
        /// </summary>
        /// <param name="src_Emp"></param>
        private void CheckDisabledEMP(List<SW_HR_ACCOUNT> src_Emp)
        {
            try
            {
                AddLog("開始檢查已停用員工");
                List<string> lstEmp = new List<string>();
                List<string> lstEmpInUOF = new List<string>();
                StringBuilder strDisEmp = new StringBuilder();
                SyncPO syncPO = new SyncPO();
                int DisEmp = 0;

                //員工資料(STATE = 1)
                foreach (var emp in src_Emp)
                {
                    lstEmp.Add(emp.Account.Trim());
                }

                SyncPO po = new SyncPO();
                List<TB_EB_USER> lstUOFUser = po.GetUSER();
                if (lstUOFUser != null && lstUOFUser.Count != 0)
                {
                    foreach (var user in lstUOFUser)
                    {
                        lstEmpInUOF.Add(user.ACCOUNT);
                    }
                }
                else
                {
                    SendEmailNow("檢查已停用員工 - 取得UOF員工資料異常");
                    throw new Exception("檢查已停用員工 - 取得UOF員工資料異常");
                }

                List<string> lstEmpInUOFNotIn = lstEmpInUOF.Except(lstEmp).ToList();

                //停用員工
                AddLog("停用員工為 ");
                if (lstEmpInUOFNotIn.Count != 0)
                {
                    foreach (var user in lstEmpInUOFNotIn)
                    {
                        if (user != "admin")
                        {
                            //比對員工是為停用(N)，
                            //若不存在，則該員工為UOF手動創立帳號，故不停用
                            List<SW_HR_ACCOUNT> lstResult = syncPO.Get_DIS_EMP(user);
                            if (lstResult != null && lstResult.Count != 0)
                            {
                                var item = lstUOFUser.Where(u => u.ACCOUNT == user).FirstOrDefault();

                                if (item != null)
                                {
                                    string userGuid = item.USER_GUID;
                                    string userName = item.NAME;
                                    bool isSuspended = item.IS_SUSPENDED;

                                    if (!isSuspended)
                                    {
                                        EmployeeUCO empUCO = new EmployeeUCO();
                                        EmployeeDataSet empDS = empUCO.GetEmployeeByUserGuid(userGuid);
                                        EmployeeDataSet.EmployeeRow empRow = empDS.Employee.FindByUSER_GUID(userGuid);

                                        empRow.IS_SUSPENDED = true;
                                        empRow.LAST_SUSPENDED_DATE = DateTime.Now;
                                        empUCO.Update(empDS);

                                        strDisEmp.Append($"姓名: {userName} 工號: {user} | ");
                                        DisEmp++;
                                    }
                                }
                            }
                        }
                    }
                }
                AddLog(strDisEmp.ToString());
                strDisEmp.Clear();

                AddLog($"停用員工 {DisEmp} 位");
                AddLog("檢查停用員工結束");
            }
            catch (Exception ex)
            {
                SendEmailNow($"{ex}");
                throw;
            }
        }

            private void AddLog(string log)
        {
            this.messageList.AppendFormat("{0}   {1}\r\n", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"), log);
        }

        void SendEmailNow(string log)
        {
            PrivateMessageUCO privateMessageUCO = new PrivateMessageUCO();
            UserUCO userUCO = new UserUCO();
            string userGuid = userUCO.GetGUID("admin");
            //privateMessageUCO.SendOneNewMessage("admin", "人員同步錯誤訊息", log, userGuid);
        }
    }
}
