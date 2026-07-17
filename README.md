# Amazon Redshift Serverless Analytics Workshop
**Teracloud Data Engineering Internship**

This repository contains the deliverables, dataset references, and implementation scripts for the **Workshop · Amazon Redshift Serverless · Analytics**, completed as part of the Teracloud Data Engineering internship curriculum.

---

## Workshop Overview
The objective of this workshop is to build and evaluate an end-to-end data warehousing pipeline using **Amazon Redshift Serverless** to process and analyze commercial real estate listings in Buenos Aires for 2020. 

The workflow consists of:
1. **Infrastructure Provisioning:** Deploying a transient compute layer (Workgroup `workshop-wg-jip`) and persistent storage/metadata layer (Namespace `workshop-rs-jip`) on Redshift Serverless.
2. **Data Curation & Ingestion:** Ingesting cleaned data in Parquet format from the S3 Data Lake (`s3://workshop-redshift-jip/curated/locales-en-venta/`) using Redshift's parallel `COPY` utility.
3. **Analytical Querying:** Running complex SQL aggregates to extract business insights regarding property values, gallery distributions, and neighborhood comparisons.
4. **Data Lake Loop Closure:** Exporting aggregated top-performing subsets back to the S3 `analytics/` layer using the `UNLOAD` utility.
5. **Cost & Sharing Study:** Documenting Redshift Serverless pricing mechanics (RPUs, compute vs storage decoupling) and architectural best practices for cross-team Data Sharing.

---

## Repository Structure & File Guide

Below is a guide to the files and directories in this repository:

* **[doc/](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/doc/)**
  * **[Workshop-Redshift-Serverless.md](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/doc/Workshop-Redshift-Serverless.md):** The original workshop requirements, design constraints, and technical guidelines provided for the assessment.
* **[src/](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/src/)**
  * **[REDSHIFT_REPORT.md](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/src/REDSHIFT_REPORT.md):** The final technical report summarizing the execution, containing screenshots, SQL code, system configurations, and responses to theoretical cost questions.
  * **[Consultas.ipynb](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/src/Consultas.ipynb):** Redshift notebook containing the exact SQL queries executed (DDL, COPY, validation, business queries, and UNLOAD block).
  * **[dataset/](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/src/dataset/)**
    * **[locales-en-venta-2020.csv](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/src/dataset/locales-en-venta-2020.csv):** Local raw source dataset (used for context on headers and record counts).
* **[assets/](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/assets/)**
  * **[images/](file:///c:/Users/nacho/Dev/Teracloud/09-Redshift/assets/images/):** Contains the console and query editor screenshots referenced in the final technical report.
