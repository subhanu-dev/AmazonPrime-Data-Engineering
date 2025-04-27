# Amazon Prime TV Shows and Movies Data pipeline on Azure 🎥

<img src="Screenshot 2025-04-27 100918.png" width="700px">

This project delved into the Amazon Prime Movies and TV shows titles and using this dataset to build an end to end production level pipeline.

These were the cutting edge tools on Azure that I used to build this

- Initial Files Storage on Github
- Azure Data Factory for ETL
- Azure Data Lake Storage
- Azure Databricks for data processing and transformations with the medallion architecture
- Azure Synapse for warehousing

The data was first fetched from the github with the copy function in Azure data factory. The Amazon Prime Movies and TV Shows dataset (sourced from Kaggle) contains metadata on titles, including release year, genre, duration, and ratings.

The pipeline follows a medallion architecture (bronze, silver, gold layers): <br>

🥉Bronze Layer: Raw CSV files stored in Azure Data Lake. <br>
🥈Silver Layer: Cleaned and enriched data with schema validation in the parquet format. <br>
🥇Gold Layer: Aggregated, business-ready tables for analytics in the delta format. <br>

Kaggle dataset link 🔗 : https://www.kaggle.com/datasets/shivamb/amazon-prime-movies-and-tv-shows

The data fetched from github is stored into the bronze container as raw csv files.

<img src="https://github.com/user-attachments/assets/01f1ae1b-4b67-4809-b224-d25afe28cd79" width="700px">

A challenge I had faced when running databricks was the unavailability of VMs to run a cluster in certain location. After multiple attempts I checked the availability of the VMs using azure CLI and assigend a 4 core 16gigs VM for a single node cluster in databricks. 

<img src="https://github.com/user-attachments/assets/3034a13a-06db-4b99-b0ac-b50f76f0b751" width="700px">

Then the databricks was connected to ADLS storage using client secrets to access the data in our raw data files in the bronze container . silver layer transformations were done on this data fetched and after doing some EDA and schema transformations, the data was written back to the silver container in the parquet format.

P.S.: These pyspark notebooks are also uploaded to this github as "silver_layer_transformations" etc. 

<img src="https://github.com/user-attachments/assets/b5866940-b97e-49ba-a38e-091b503ac01a" width="700px">
<br>

Then the data is fetched back again from the silver layer and final preprocessing, creation of new columns such as genre, changing the date format to a consistent format etc were applied. The results were stored back into gold containers in the delta format. 

<img src="https://github.com/user-attachments/assets/ccda5c1e-f56c-486c-9d6e-9d14917b7106" width="700px">

<br>
ADLS Storage containers <br>

<img src="https://github.com/user-attachments/assets/95ff6741-d221-46df-9333-1fedb4842955" width="700px">
<br> 
The results can then be queried in synapse by using the lake from the gold layer data. Synapse setup is set to use built-in serverless pools <br>
<br>

<img src="https://github.com/user-attachments/assets/0ca05fe9-1b69-4227-83e5-be029c70f44e" width="700px">
<br>

The next step is connecting power BI to build dashboards querying gold tables via Synapse

---
Made with ❤️ by [Subhanu](https://github.com/subhanu-dev)

