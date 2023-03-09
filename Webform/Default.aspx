<%@ Page Title="" Language="C#" MasterPageFile="~/Master/DefaultMasterPage.master" AutoEventWireup="true" CodeFile="Default.aspx.cs" Inherits="CDS_Sync_Default" %>

<asp:Content ID="Content1" ContentPlaceHolderID="ContentPlaceHolder1" Runat="Server">
    <script type="text/javascript">
        function DisableBtn() {
            document.getElementById("btnExecute").disabled=true;
        }

    </script>
    <table class="PopTable">
        <tr>            
            <td>
                <asp:Label ID="Label1" runat="server" Text="Email">手動執行組織同步</asp:Label>
            </td>
            <td>
                <asp:Button ID="btnExecute" runat="server" Text="開始同步"  OnClick="btnExecute_Click"/>
            </td>
        </tr>
         <tr>
            <td>
                <asp:Label ID="Label4" runat="server" Text="執行訊息"></asp:Label>
            </td>
            <td>
                 <asp:TextBox ID="txtMessage" runat="server" TextMode="MultiLine" Height="300px" Width="50%"></asp:TextBox>
            </td>
        </tr>
    </table>
</asp:Content>

