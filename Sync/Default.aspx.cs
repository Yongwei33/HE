using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Ede.Uof.EIP.SystemInfo;
using Ede.Uof.Utility.Log;
using Ede.Uof.Utility.Page;
using Lib.Sync.Sync;

public partial class CDS_Sync_Default : BasePage
{
    protected void Page_Load(object sender, EventArgs e)
    {
        this.btnExecute.Attributes.Add("onclick", "this.value='同步中...';this.disabled=true;" + ClientScript.GetPostBackEventReference(btnExecute, "").ToString());
    }
    protected void btnExecute_Click(object sender, EventArgs e)
    {

        SyncUCO syncUco = new SyncUCO();
        syncUco.RunSyncTask(Current.Account);

        string message = syncUco.SyncLog;
        txtMessage.Text = message;
        btnExecute.Enabled = true;

        Logger.Write("Lib_Sync","手動執行組織同步, " + message);

    }
}