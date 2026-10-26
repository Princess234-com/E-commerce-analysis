# E-Commerce Analysis

##  Technical Architecture & Data Engineering

An AWS-based data engineering and machine learning portfolio project using synthetic e-commerce data. This project contains events, product catalogs, order records, user metadata, and profiles into queryable analytical datasets, then segments customer behavior using machine learning.
```
┌─────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
│      BRONZE LAYER       │ ──►  │      SILVER LAYER       │ ──►  │    GOLD / ANALYTICS     │
│  Raw CSV Files in S3    │      │  AWS Glue PySpark ETL   │      │ Amazon Athena & Power BI│
│ (Users, Events, Orders) │      │ (Partitioned Parquet)   │      │ RFM ML Segmentation     │
└─────────────────────────┘      └─────────────────────────┘      └─────────────────────────┘

```

* **Bronze Storage:** Raw unstructured and semi-structured CSV datasets (`users`, `products`, `user_preferences`, `events`, `orders`, and `user_profiles`) ingested into Amazon S3.


* **AWS Glue PySpark ETL Pipeline:**
* Handled schema adjustments and header shifts (e.g., sanitizing index artifacts in `user_preferences`).


* Cast string timestamps and dates into native Spark `TimestampType` and `DateType`.
* Enforced primary key deduplication across atomic event and order tables.


* **Optimized Storage Format:** Converted raw logs into partitioned **Apache Parquet files with Snappy compression** (partitioned by `/year=/month=/day=`), drastically reducing storage overhead and optimizing query scan costs in Amazon Athena.

---

##  Serverless Analytics & Power BI Dashboard

Using **Amazon Athena**, serverless SQL queries were executed across the Silver layer to extract actionable retail business metrics, which were then modeled into an interactive **Power BI Dashboard**:

* **Cart Abandonment Rate:** Analyzed across devices (`mobile` 84.87%, `tablet` 84.77%, `desktop` 84.70%), proving that high drop-off (~84.8%) is driven by systemic checkout friction rather than device layout responsiveness.
* **Category Conversion Funnel:** Tracked user drop-off across `product_views` $\rightarrow$ `cart_additions` $\rightarrow$ `purchases`, identifying categories like **Books** (18.39% conversion) and **Gaming** (18.30%) as top performers.
* **Customer Lifetime Value (CLV) by Acquisition Channel:** Discovered that **Affiliate** marketing drives the highest customer value (£2,506.25 average CLV, £505.97 AOV), outperforming paid social channels.

---

##  Machine Learning: RFM Customer Segmentation & PCA Clustering
<img width="1427" height="857" alt="image" src="https://github.com/user-attachments/assets/e4b89c05-a202-4908-a273-a3c3eb486c72" />


To move beyond generalized marketing, an unsupervised machine learning pipeline was built using **Scikit-Learn** to cluster users based on **Recency, Frequency, and Monetary (RFM)** metrics alongside behavioral engagement features.

* **Feature Engineering:** Extracted purchasing frequencies, total revenues, and engagement counts from Silver/Gold tables, applying log-transformations and `StandardScaler` to handle skewed monetary distributions.
* **Dimensionality Reduction & Clustering:** Applied **Principal Component Analysis (PCA)** to reduce the high-dimensional customer space into two orthogonal axes:


* **PCA 1 (Horizontal Axis):** *Overall Customer Value & Engagement* (capturing spend magnitude and transaction frequency).


* **PCA 2 (Vertical Axis):** *Recency & Behavioral Pattern* (capturing temporal activity distribution).




* Trained a **K-Means Clustering** model ($K=4$) to segment the user base into distinct operational tiers:





### **Cluster Breakdown & Strategic Playbooks:**

1. **VIP Champions (Purple):** High positive alignment on PCA 1 (high spend, high frequency). Linear patterns along PCA 2 reflect consistent, structured repurchase cycles. *Strategy:* Target with exclusive loyalty perks and high-value upsells.


2. **Loyal Mid-Tier (Dark Teal):** Active mid-value users with steady engagement. *Strategy:* Push cross-selling bundles to elevate them into VIP status.


3. **Casual Shoppers (Green):** Concentrated on the mid-to-lower end of engagement. *Strategy:* Re-engage via targeted cart recovery campaigns and personalized discounts.


4. **Inactive / Outliers (Light Green):** Isolated low-engagement accounts. *Strategy:* Run automated win-back emails or suppress from high-cost acquisition ad spend.



---

##  Technologies

* **Cloud & Data Engineering:** AWS S3, AWS Glue, PySpark, Amazon Athena, Partitioned Parquet, Medallion Architecture.
* **Data Science & Machine Learning:** Scikit-Learn, K-Means Clustering, Principal Component Analysis (PCA)
* **Business Intelligence:** Power BI
