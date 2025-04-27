# Amazon Prime TV Shows and Movies Data pipeline on Azure 🎥

<img src="Screenshot 2025-04-27 100918.png" width="700px">

This project delved into the Amazon Prime Movies and TV shows titles and using this dataset to build an end to end production level pipeline.

These were the cutting edge tools on Azure that I used to build this

- Initial Files Storage on Github
- Azure Data Factory for ETL
- Azure Data Lake Storage
- Azure Databricks for data processing and transformations with the medallion architecture
- Azure Synapse for warehousing

The data was first fetched from the github with the copy

![Screenshot 2025-04-24 120145](https://github.com/user-attachments/assets/01f1ae1b-4b67-4809-b224-d25afe28cd79)


![Screenshot 2025-04-22 184658](https://github.com/user-attachments/assets/b5866940-b97e-49ba-a38e-091b503ac01a)

![Screenshot 2025-04-24 132047](https://github.com/user-attachments/assets/3034a13a-06db-4b99-b0ac-b50f76f0b751)


![Screenshot 2025-04-25 204444](https://github.com/user-attachments/assets/ccda5c1e-f56c-486c-9d6e-9d14917b7106)

The pipeline follows a medallion architecture (bronze, silver, gold layers):
Bronze Layer: Raw CSV files stored in Azure Data Lake.
Silver Layer: Cleaned and enriched data with schema validation in the parquet format.
Gold Layer: Aggregated, business-ready tables for analytics in the delta format.

ADLS Storage containers

![image](https://github.com/user-attachments/assets/95ff6741-d221-46df-9333-1fedb4842955)

The results can be queried in synapse by using the lake from the gold layer data.
![image](https://github.com/user-attachments/assets/0ca05fe9-1b69-4227-83e5-be029c70f44e)

<img src="Screenshot 2025-04-27 100918.png" width="700px">

---
Made with ❤️ by [Subhanu](https://github.com/subhanu-dev)

