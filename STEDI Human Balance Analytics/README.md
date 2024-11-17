# STEDI Human Balance Analytics
**Data Engineering a data lakehouse solution for sensor data that trains a machine learning model for the STEDI team**

## About
STEDI has received data from millions of early adopters who are eager to purchase the **STEDI Step Trainers** and use them. Several customers have already received their Step Trainers, installed the mobile application, and started using them to test their balance. The **Step Trainer** is a motion sensor that records the distance of detected objects. The app uses a mobile phone **accelerometer** to detect motion in the **X**, **Y**, and **Z** directions.

The STEDI team aims to use this motion sensor data to train a machine learning model that detects steps in real-time. Privacy is a primary concern, as only data from customers who agree to share their information for research purposes will be used. 

Our goal is to extract the data from **STEDI Step Trainer sensors** and the **mobile app**, then curate it into a **Data Lakehouse solution on AWS**. This curated data will be used by data scientists to train the learning model.

## Resources
To build the data lakehouse solution in the cloud, we will utilize:
- **Python and Spark**
- **AWS Glue**
- **AWS Athena**
- **AWS S3**
  
## Project Data 
The STEDI project has three main **JSON data sources** to process from the Step Trainer:

1. **Customer Records**
   - This data comes from fulfillment and the STEDI website.
   - Fields include:
     - `serialnumber`
     - `sharewithpublicasofdate`
     - `birthday`
     - `registrationdate`
     - `sharewithresearchasofdate`
     - `customername`
     - `email`
     - `lastupdatedate`
     - `phone`
     - `sharewithfriendsasofdate`
  
2. **Step Trainer Records**
   - Data from the motion sensor.
   - Fields include:
     - `sensorReadingTime`
     - `serialNumber`
     - `distanceFromObject`
  
3. **Accelerometer Records**
   - Data from the mobile app's accelerometer.
   - Fields include:
     - `timeStamp`
     - `user`
     - `x`
     - `y`
     - `z`

## Workflow
Using **AWS Glue**, **AWS S3**, **Python**, and **Spark**, we will create Python scripts to build a Data Lakehouse solution on AWS. This solution will satisfy the requirements of STEDI’s data scientists.

Refer to the flowchart below to better understand the workflow:
![image](https://github.com/user-attachments/assets/5bbcd9fb-6d71-4fd3-9f09-75b796b11554)


## Data Engineering steps
1. **Create the S3 Buckets** using the AWS S3 UI. The structure under the `stedi` S3 bucket will look like this:
      ```bash
        customer/
        - landing/
        - trusted/
        - curated/
        accelerometer/
        - landing/
        - trusted/
        step_trainer/
        - landing/
        - trusted/
        - curated/
      ```
3. **Copy Data from GitHub to S3 Buckets**:
- Clone the repository:
  ```bash
  git clone https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git stedi-project-starter-code
  ```
- Copy data to S3 buckets:
  ```bash
  aws s3 cp /home/cloudshell-user/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/customer/landing s3://stedi-datalake/customer/landing/ --recursive
  aws s3 cp /home/cloudshell-user/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing s3://stedi-datalake/accelerometer/landing/ --recursive
  aws s3 cp /home/cloudshell-user/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/step_trainer/landing s3://stedi-datalake/step_trainer/landing/ --recursive
  ```

3. **Landing Zone**:
- Create a **Crawler** using **AWS Glue** to pick up the schema and perform **Exploratory Data Analysis (EDA)** on the data using **AWS Athena**.

4. **Trusted Zone**:
- **Sanitize Customer Data** from the Website (Landing Zone) to store only records of customers who agreed to share data for research purposes.
  - Create a Glue Table called `customer_trusted`.
- **Sanitize Accelerometer Data** from the Mobile App (Landing Zone) to store data only from customers who agreed to share data.
  - Create a Glue Table called `accelerometer_trusted`.
  - Perform an **inner join** between the `customer_trusted` and `accelerometer_landing` data using **email**.
  - Only pick records after the consent date of the user.
- **Step Trainer Trusted**:
  - Create a `step_trainer_trusted` table with data for customers who have accelerometer data and agreed to share their data for research.
  - Perform an **inner join** between the `step_trainer_landing` and `customer_curated` data using **serial numbers**.

5. **Curated Zone**:
- **Sanitize the Customer Data** (Trusted Zone) and create a Glue Table (Curated Zone) for customers who have accelerometer data and agreed to share their data.
  - Create the `customers_curated` table.
- **Anonymize the Data** to ensure it is not subject to **GDPR**.
- Create an aggregated table with **Step Trainer Readings** and associated **Accelerometer Readings** for the same timestamp, but only for customers who agreed to share their data.
  - Create a Glue Table called `machine_learning_curated`.
  - The `machine_learning_curated.py` script performs an **inner join** between the `step_trainer_trusted` and `accelerometer_trusted` data by `sensorReadingTime` and `timestamp`.

Refer to the relationship diagram below to understand the desired state:

![image](https://github.com/user-attachments/assets/6ac15c2d-ffe1-417c-a346-829522248ec6)

## Challenges

1. **Glue Filter Node** wasn't filtering data as expected. To resolve this, the **SQL node** was used instead.
2. Data was not being read from AWS S3 buckets during the ETL process for trusted and curated data warehouses. This issue was solved by using **Glue DataCatalog** to access the data.
3. **Glue Join Node** wasn’t working as expected, so the **SQL node** was used as a workaround.
