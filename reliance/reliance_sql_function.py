import os
import pandas as pd
import pymssql

 
def fetch_client_data(client_name):
    # Database credentials (must be loaded from environment variables)
    server = os.environ.get('AURA_DB_SERVER')
    database = os.environ.get("AURA_DB_NAME")
    username = os.environ.get("AURA_DB_USER")
    password = os.environ.get("AURA_DB_PASSWORD")

    # Check for missing environment variables
    missing_vars = []
    if not server:
        missing_vars.append('AURA_DB_SERVER')
    if not database:
        missing_vars.append('AURA_DB_NAME')
    if not username:
        missing_vars.append('AURA_DB_USER')
    if not password:
        missing_vars.append('AURA_DB_PASSWORD')
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

    # Establish the connection to the SQL Server using pymssql
    try:
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        
        # Create a cursor object
        cursor = conn.cursor()
        
        query = f"""Select 
                IsNull(IsComplete , '') As IsComplete , IsNull(StyleId , '') As StyleId , IsNull(SKUNo , '') As SKUNo 
                , IsNull(ImageName , '') As ImageName , IsNull(ImageExt , '') As ImageExt
                , IsNull(GrpGroupName , '') As MainGroupPrdctCtg, IsNull(GrpName , '') As SubGroupPrdctCtg, IsNull(StyleCode , '') As StyleCode , IsNull(StyleDate , '') As StyleDate
                , IsNull(BaseCollectionName , '') As BaseCollectionName , IsNull(BaseCollectionCode , '') As BaseCollectionCode 
                , IsNull(Restricted , '') As Restricted , IsNull(CustomerId , '') As  CustomerId , IsNull(LegalName , '') As  LegalName
                , IsNull(PartyName , '') As PartyName , IsNull(PartyCode , '') As PartyCode
                , IsNull(BaseMetal , '') As BaseMetal , IsNull(BaseStone , '') As BaseStone 
                , IsNull(ItemSize , '') As ItemSize , IsNull(StampingInstruction , '') As StampingInstruction 
                , IsNull(CustomerProductionInstruction , '') As CustomerProductionInstruction
                , IsNull(DesignProductionInstruction , '') As DesignProductionInstruction
                , IsNull(ClientDiamondPcs , 0) As ClientDiamondPcs , IsNull(ClientDiamondWt , 0) As ClientDiamondWt
                , IsNull(ColorstonePcs , 0) As ColorstonePcs , IsNull(ColorStoneWt , 0) As ColorStoneWt
                , IsNull(CZPcs , 0) As CZPcs , IsNull(CZWt , 0) As CZWt
                , IsNull(DiamondPcs , 0) As DiamondPcs , IsNull(DiamondWt , 0) As DiamondWt
                , IsNull(NetWt , 0) As NetWt , IsNull(NetWtBase , 0) As NetWtBase , IsNull(NetWtNotBase , 0) As NetWtNotBase , IsNull(GrossWt , 0) As GrossWt
                , IsNull(MakeType , '') As MakeType , IsNull(Manufacturer , '') As Manufacturer
                From (
                Select sm.IsComplete , sm.StyleId , sm.StyleCode as SKUNo
                , sm.ImageName , sm.ImageExt  , mp.GrpGroupName , mp.GrpName , s.StyleCode , sm.StyleDate As StyleDate
                , IsNull((Select Coll.DesgName From [AURESJEP].[dbo].DesignMst As Coll With(Nolock) 
                Where Coll.DesgNo=smsum.BaseDesgNo) , '') As BaseCollectionName
                , IsNull((Select Coll.DesgCode From [AURESJEP].[dbo].DesignMst As Coll With(Nolock) 
                Where Coll.DesgNo=smsum.BaseDesgNo) , '') As BaseCollectionCode
                , sm.Restricted , sm.CustomerId 
                , (Case When pm.LegalName <> '' Then  pm.LegalName Else pm.FirmName End ) As LegalName 
                , (Case When 0=1 Then pm.PartyCode Else pm.FirmName End) As PartyName , pm.PartyCode
                , (Select dIv.ItemCode From [AURESJEP].[dbo].spm_itemView As dIv Where dIv.ItemId=smsum.BaseMetalId) BaseMetal
                , (Select dIv.ItemCode From [AURESJEP].[dbo].spm_itemView As dIv Where dIv.ItemId=smsum.BaseStoneId) BaseStone
                , (Select cm.CommonMasterCode From [AURESJEP].[dbo].Spm_CommonMaster as Cm With (Nolock) 
                where Cm.CommonMasterId=sm.ItemSizeId) as ItemSize
                , sm.StampingInstruction , sm.CustomerProductionInstruction , sm.DesignProductionInstruction
                , IsNull(smSum.TotCDiaPc , 0) As ClientDiamondPcs , IsNull(smSum.TotCDiaWt , 0) As ClientDiamondWt
                , IsNull(SmSum.TotImiPc , 0) As ColorstonePcs , IsNull(SmSum.TotImiwt , 0) As ColorStoneWt
                , IsNull(SmSum.TotCzPc , 0) As CZPcs , IsNull(SmSum.TotCzWt , 0) As CZWt
                , IsNull(SmSum.TotDiaPc , 0) As DiamondPcs , IsNull(SmSum.TotDiaWt , 0) As DiamondWt
                , SmSum.NetWt+SmSum.NetWtNotBase As NetWt , smSum.NetWt As NetWtBase , smSum.NetWtNotBase As NetWtNotBase , IsNull(SmSum.GrossWt , 0) As GrossWt
                , IsNull((Select Mtm.MakeTypeName From [AURESJEP].[dbo].MakeTypeMst As Mtm With(Nolock) Where Mtm.MakeTypeNo=Sm.MakeTypeNo) , '') As MakeType
                , IsNull((Select Case When 0 = 1 Then Pm.PartyCode Else Pm.FirmName End From [AURESJEP].[dbo].PartyMst As Pm With (Nolock) 
                Where Pm.PartyNo=Sm.CompanyId) , '') As Manufacturer
                From [AURESJEP].[dbo].PartyStyleMst sm With (Nolock)
                Inner Join [AURESJEP].[dbo].PartyStyleMstSummary smsum With (Nolock) on smsum.StyleId=sm.StyleId 
                Inner Join [AURESJEP].[dbo].StyleMst s With (Nolock) on s.StyleId=sm.ReferenceId 
                Inner Join [AURESJEP].[dbo].MainProduct_View mp With (Nolock) on sm.GrpNo=mp.GrpNo 
                Inner Join [AURESJEP].[dbo].PartyMst pm With (Nolock) on pm.PartyNo=sm.CustomerId 
                Where sm.StyleId <> '' 
                And (Case When pm.LegalName <> '' Then  pm.LegalName Else pm.FirmName End) Like '%{client_name}%'
                ) An 
                Where StyleId <> '' 
                Order By LegalName , GrpName , SKUNo"""

        df = pd.read_sql(query, conn)

        if client_name == 'Titan':
            query1 = """Select * From [AuraDb].[dbo].[Tbl_GCMaxDsgList] Order By DsgId"""
            query2 = """Select * From [AuraDb].[dbo].[Tbl_NoosBufferBif] Order By [Bifurcation]"""

            # Fetch the data into DataFrames using pandas' read_sql function
            gcmax = pd.read_sql(query1, conn)
            gcmax.rename(columns={"OldSkuNo": "Sku"}, inplace=True)
            noosebuffer = pd.read_sql(query2, conn)
            
            return df, gcmax, noosebuffer
        elif client_name == 'Reliance':
            
            query1 = """Select * From [AuraDb].[dbo].[Tbl_RRLDsgCdMst] Order By RRLDsgCd , [AuraDsgCd]"""
            validator_df = pd.read_sql(query1, conn)
            return df, validator_df

    except pymssql.Error as e:
        print(f"An error occurred while executing the query for client {client_name}: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    finally:
        # Ensure the connection is closed
        conn.close()
