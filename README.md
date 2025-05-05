# Delta Lake Tables in Apache Spark on Microsoft Fabric

This lab demonstrates how to use Delta Lake tables within an Apache Spark environment on Microsoft Fabric. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on top of data lakes.

**Estimated Completion Time:** 45 minutes

**Prerequisites:**

* A Microsoft Fabric trial enabled.
* Access to a web browser.

## Steps

### 1. Create a Workspace

1.  Navigate to the [Microsoft Fabric home page](https://app.fabric.microsoft.com/home?experience=fabric) and sign in with your Fabric credentials.
2.  In the left-hand menu bar, select **Workspaces** (🗇).
3.  Create a **New workspace** with a name of your choice. Ensure you select a licensing mode that includes Fabric capacity (Trial, Premium, or Fabric).
4.  Once the workspace is created and opened, it should be empty.

### 2. Create a Lakehouse and Upload Data

1.  In the left-hand menu bar, select **Create**. Under the **Data Engineering** section, choose **Lakehouse**. Give it a unique name.
    * **Note:** If the **Create** option is not visible, click the ellipsis (**...**) first.
2.  Download the `products.csv` data file from [https://github.com/MicrosoftLearning/dp-data/raw/main/products.csv](https://github.com/MicrosoftLearning/dp-data/raw/main/products.csv) and save it to your local computer (or lab VM).
3.  In your Fabric lakehouse, within the **Explorer** pane, next to the **Files** folder, click the **...** menu and select **New subfolder**. Name the new folder `products`.
4.  In the **...** menu for the newly created `products` folder, select **upload** and upload the `products.csv` file from your local machine.
5.  After uploading, verify that the `products.csv` file is present in the `products` folder.

### 3. Explore Data in a DataFrame

1.  Create a **New notebook**.
2.  Select the first cell (code cell) and use the **M↓** button in the top-right toolbar to convert it to a markdown cell.
3.  Edit the markdown cell with the following content:

    ```markdown
    # Delta Lake tables
    Use this notebook to explore Delta Lake functionality
    ```

4.  Add a new code cell and paste the following code to read the `products.csv` data into a Spark DataFrame with a defined schema:

    ```python
    from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

    # define the schema
    schema = StructType() \
        .add("ProductID", IntegerType(), True) \
        .add("ProductName", StringType(), True) \
        .add("Category", StringType(), True) \
        .add("ListPrice", DoubleType(), True)

    df = spark.read.format("csv").option("header","true").schema(schema).load("Files/products/products.csv")
    # df now is a Spark DataFrame containing CSV data from "Files/products/products.csv".
    display(df)
    ```

5.  Run the code cell using the **Run cell** (▷) button.

### 4. Create Delta Tables

#### 4.1. Create a Managed Table

1.  Add a new code cell below the previous output.
2.  Enter and run the following code to save the DataFrame as a managed Delta table:

    ```python
    df.write.format("delta").saveAsTable("managed_products")
    ```

3.  In the Lakehouse explorer pane, **Refresh** the **Tables** folder and expand the **Tables** node to confirm that the `managed_products` table has been created (indicated by a triangle icon).
    * The data files for managed tables are stored in the `Tables` folder within a subfolder named `managed_products`, including Parquet files and the `_delta_log` folder.

#### 4.2. Create an External Table

1.  In the Lakehouse explorer pane, in the **...** menu for the **Files** folder, select **Copy ABFS path**.
2.  In a new code cell, paste the copied ABFS path.
3.  Append the following code to the pasted path, replacing `abfs_path` with the actual path:

    ```python
    df.write.format("delta").saveAsTable("external_products", path="abfs_path/external_products")
    ```

    The full path should look similar to:

    ```
    abfss://<workspace>@<tenant-onelake>[.dfs.fabric.microsoft.com/](https://.dfs.fabric.microsoft.com/)<lakehousename>.Lakehouse/Files/external_products
    ```

4.  Run the code cell. This saves the DataFrame as an external table in the `Files/external_products` folder.
5.  **Refresh** the **Tables** folder in the Lakehouse explorer and verify that the `external_products` table (containing schema metadata) has been created.
6.  **Refresh** the **Files** folder and confirm that the `external_products` folder (containing the data files) has been created.

### 5. Compare Managed and External Tables

1.  Add a new code cell and run the following SQL query using the `%%sql` magic command:

    ```sql
    %%sql
    DESCRIBE FORMATTED managed_products;
    ```

2.  Examine the **Location** property in the results. Note that the path ends with `/Tables/managed_products`.
3.  Modify the `DESCRIBE` command in a new code cell to inspect the external table:

    ```sql
    %%sql
    DESCRIBE FORMATTED external_products;
    ```

4.  Run the cell and observe the **Location** property. Notice that the path ends with `/Files/external_products`.
5.  In a new code cell, run the following code to drop both tables:

    ```sql
    %%sql
    DROP TABLE managed_products;
    DROP TABLE external_products;
    ```

6.  **Refresh** the **Tables** folder in the Lakehouse explorer to confirm that no tables are listed.
7.  **Refresh** the **Files** folder and verify that the `external_products` folder (containing data and `_delta_log`) has **not** been deleted. This demonstrates that dropping an external table only removes the metadata, not the underlying data.

### 6. Use SQL to Create a Delta Table

1.  Add a new code cell and run the following SQL command to create a Delta table based on the existing data in the `Files/external_products` folder:

    ```sql
    %%sql
    CREATE TABLE products
    USING DELTA
    LOCATION 'Files/external_products';
    ```

2.  **Refresh** the **Tables** folder in the Lakehouse explorer. Expand the **Tables** node and verify that a new table named `products` is listed. Expand the table to view its schema.

3.  Add another code cell and run the following query to view the data in the newly created `products` table:

    ```sql
    %%sql
    SELECT * FROM products;
    ```

### 7. Explore Table Versioning

1.  Add a new code cell and run the following SQL command to update the `products` table, applying a 10% price reduction to 'Mountain Bikes':

    ```sql
    %%sql
    UPDATE products
    SET ListPrice = ListPrice * 0.9
    WHERE Category = 'Mountain Bikes';
    ```

2.  Add another code cell and run the following command to view the transaction history of the `products` table:

    ```sql
    %%sql
    DESCRIBE HISTORY products;
    ```

    The output will show the different versions and operations performed on the table.

3.  Add another code cell and run the following PySpark code to retrieve and display the current and original (version 0) data:

    ```python
    delta_table_path = 'Files/external_products'# Get the current data
    current_data = spark.read.format("delta").load(delta_table_path)
    display(current_data)# Get the version 0 data
    original_data = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
    display(original_data)
    ```

    You should see two result sets: one with the updated prices and one with the original prices.

### 8. Analyze Delta Table Data with SQL Queries

1.  Add a new code cell and run the following SQL code to create a temporary view and select aggregated data:

    ```sql
    %%sql
    -- Create a temporary view
    CREATE OR REPLACE TEMPORARY VIEW products_view
    AS
        SELECT Category, COUNT(*) AS NumProducts, MIN(ListPrice) AS MinPrice, MAX(ListPrice) AS MaxPrice, AVG(ListPrice) AS AvgPrice
        FROM products
        GROUP BY Category;

    SELECT *
    FROM products_view
    ORDER BY Category;
    ```

2.  Add a new code cell and run the following SQL query to find the top 10 categories with the most products:

    ```sql
    %%sql
    SELECT Category, NumProducts
    FROM products_view
    ORDER BY NumProducts DESC
    LIMIT 10;
    ```

    You can switch to the **Chart** view below the output to visualize the results as a bar chart.

3.  Alternatively, add a new code cell and run the following PySpark code to achieve a similar analysis:

    ```python
    from pyspark.sql.functions import col, desc

    df_products = spark.sql("SELECT Category, MinPrice, MaxPrice, AvgPrice FROM products_view").orderBy(col("AvgPrice").desc())
    display(df_products.limit(6))
    ```

### 9. Use Delta Tables for Streaming Data

1.  Add a new code cell and run the following code to set up a streaming data source:

    ```python
    from notebookutils import mssparkutils
    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    # Create a folder
    inputPath = 'Files/data/'
    mssparkutils.fs.mkdirs(inputPath)

    # Create a stream that reads data from the folder, using a JSON schema
    jsonSchema = StructType([
        StructField("device", StringType(), False),
        StructField("status", StringType(), False)
    ])
    iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

    # Write some event data to the folder
    device_data = '''{"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev2","status":"error"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"error"}
    {"device":"Dev2","status":"ok"}
    {"device":"Dev2","status":"error"}
    {"device":"Dev1","status":"ok"}'''

    mssparkutils.fs.put(inputPath + "data.txt", device_data, True)

    print("Source stream created...")
    ```

    Ensure the message "Source stream created…" is displayed.

2.  In a new code cell, add and run the following code to write the stream to a Delta table:

    ```python
    # Write the stream to a delta table
    delta_stream_table_path = 'Tables/iotdevicedata'
    checkpointpath = 'Files/delta/checkpoint'
    deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
    print("Streaming to delta sink...")
    ```

3.  In a new code cell, run the following SQL query to view the data in the streaming Delta table:

    ```sql
    %%sql
    SELECT * FROM IotDeviceData;
    ```

4.  Add a new code cell and run the following code to add more data to the streaming source:

    ```python
    # Add more data to the source stream
    more_data = '''{"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"error"}
    {"device":"Dev2","status":"error"}
    {"device":"Dev1","status":"ok"}'''

    mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)
    ```

5.  Re-run the code cell with the SQL query from step 9.3 to see the updated data in the `IotDeviceData` table.

6.  In a new code cell, add and run the following code to stop the streaming query:

    ```python
    deltastream.stop()
    ```

### 10. Clean Up Resources

1.  If you have finished exploring, you can delete the workspace created for this exercise.
2.  In the left-hand bar, select the icon for your workspace.
3.  In the **...** menu on the toolbar, select **Workspace settings**.
4.  In the **General** section, choose **Remove this workspace**.

This lab provided a hands-on experience with creating, managing, querying, and using Delta Lake tables for both batch and streaming data processing within Microsoft Fabric.
