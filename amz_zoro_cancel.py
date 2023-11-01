# %%
# fiter -- exp ship date by date -- past four days only

# %%
def amazon_zoro_cancel():

    import tempfile
    import os
    from io import StringIO
    from azure.storage.blob import BlobServiceClient, BlobClient, BlobLeaseClient

    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    

    connectionString1 = "DefaultEndpointsProtocol=https;AccountName=nfinsdata;AccountKey=fkIjwa6uiAZBanIeeK8MtqdrGg/04sXKgCGCCHDX0yM+D6h/slJO2cRs5l1kb/k+14wyJtG2elvs+AStl1f7vQ==;EndpointSuffix=core.windows.net"
    blobConnect = BlobServiceClient.from_connection_string(connectionString1)
    expShipContainer = "exp-ship-date"
    expShipConnect = blobConnect.get_container_client(expShipContainer)
    amzZoroContainer = "amz-zoro-cancel"
    amzZoroConnect = blobConnect.get_container_client(amzZoroContainer)
    outputCont = "output-automation"
    # outputContConnect = blobConnect.get_container_client(outputCont)

    # Blob files names in storage
    expOpBlob = "exp_ship_date_op.csv"
    emiSupplyBlob = "customsearch_cancellations_50min.csv"
   

    #change to match what container connect var
    exp_dataBlob_client = expShipConnect.get_blob_client(expOpBlob)
    emiSupplyBlob_client = amzZoroConnect.get_blob_client(emiSupplyBlob)
  
    #download blob data
    exp_dataBlob_data = exp_dataBlob_client.download_blob().content_as_text()
    emi_supplyBlob_data = emiSupplyBlob_client.download_blob().content_as_text()
   

    #parse as string into DFs
    expected_ship_output = pd.read_csv(StringIO(exp_dataBlob_data))
    emi_sku = pd.read_csv(StringIO(emi_supplyBlob_data))


    expected_ship_output['Sales Order Date'] = pd.to_datetime(expected_ship_output['Sales Order Date'])
    four_days_ago = datetime.now() - timedelta(days=4)
    expected_ship_output=expected_ship_output[expected_ship_output['Sales Order Date'] >= four_days_ago]

    for index, rows in expected_ship_output.iterrows():
        sku = rows["SKU"]
        if sku in emi_sku["Name"].values:
            rows["Name"] = "EMI Supply - for Amazon"
            expected_ship_output.at[index, "Name"] = "EMI Supply - for Amazon"
    columns_to_drop = ["Blanket Order","Private Notes", "Outsourced Item",  "Outsourced Item.1"]
    expected_ship_output = expected_ship_output.drop(columns= columns_to_drop)
    expected_ship_output['Allocation Status'] = expected_ship_output['Allocation Status'].fillna('')
    expected_ship_output = expected_ship_output[expected_ship_output['Name'].str.contains('Amazon|Zoro', case = False, regex= True)]
    expected_ship_output1 = expected_ship_output[expected_ship_output['Allocation Status'].str.contains('Order More|Available On Purchase Order|Parent on Purchase order', case = False, regex= True)]
    expected_ship_output2 = pd.DataFrame(expected_ship_output)
    expected_ship_output2 = expected_ship_output[expected_ship_output['Name'].str.contains('Amazon', case = False, regex= True)]
    expected_ship_output2 = expected_ship_output2[expected_ship_output2['Allocation Status'].str.contains('Parent In Inv - Ready to convert', case = False, regex= True)]
    expected_ship_output2['key']=  expected_ship_output2["Name"]+expected_ship_output2["SKU"]+expected_ship_output2['Ship Date'].astype(str)
    expected_ship_output2["Dollar value"]= expected_ship_output2.groupby('key')['$$ Open Amount'].transform('sum')
    expected_ship_output2 = expected_ship_output2[expected_ship_output2["Dollar value"]< 100]
    expected_ship_output3 = pd.DataFrame(expected_ship_output)
    expected_ship_output3 = expected_ship_output[expected_ship_output['Name'].str.contains('Zoro', case = False, regex= True)]
    expected_ship_output3 = expected_ship_output3[expected_ship_output3['Allocation Status'].str.contains('Parent In Inv - Ready to convert', case = False, regex= True)]
    expected_ship_output3 = expected_ship_output3[expected_ship_output3["$$ Open Amount"]< 50]
    expected_ship_output4 = pd.DataFrame(expected_ship_output)
    expected_ship_output4 = expected_ship_output4[expected_ship_output4['Name'].str.contains('EMI', case = False, regex= True)]
    expected_ship_output4 = expected_ship_output4[expected_ship_output4["$$ Open Amount"]< 50]
    appended_df = pd.concat([expected_ship_output1, expected_ship_output2, expected_ship_output3, expected_ship_output4], axis=0)
    appended_df = appended_df.sort_values(by = "Priority Order")
    appended_df = appended_df[appended_df['$$ Open Amount']!=0]
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        appended_df.to_csv(temp_file.name,index=False)
        outputPath = temp_file.name

        #this is the output to create in blob storage, change container  as defined above and file name as needed.
    uploadBlobClient = blobConnect.get_blob_client(container=amzZoroContainer, blob="amazon_zoro_cancellation_op.csv")
    uploadBlobClient = blobConnect.get_blob_client(container=outputCont, blob = "amazon_zoro_cancellation.csv")
    with open(file=outputPath,mode="rb") as outputData:
        uploadBlobClient.upload_blob(outputData, overwrite=True)

amazon_zoro_cancel()


