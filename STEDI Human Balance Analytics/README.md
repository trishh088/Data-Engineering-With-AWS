# STEDI Human Balance Analytics
Data Engineering a data lakehouse solution for sensor data that trains a machine learning model for the STEDI team 

## About
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.
Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected.
The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.
Some of the early adopters have agreed to share their data for research purposes and these customers’ Step Trainer and accelerometer data will be used in the training data for the machine learning model.
We will extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Reources
AWS Environment
We will use the data from the STEDI Step Trainer and mobile app to develop a lakehouse solution in the cloud that curates the data for the machine learning model using:
- Python and Spark
- AWS Glue
- AWS Athena
- AWS S3

## Project Data 
STEDI has three JSON data sources(opens in a new tab) to use from the Step Trainer. 
- customer
- step_trainer
- accelerometer

1. Customer Records
This is the data from fulfillment and the STEDI website.
The file contains the following fields:
    - serialnumber
    - sharewithpublicasofdate
    - birthday
    - registrationdate
    - sharewithresearchasofdate
    - customername
    - email
    - lastupdatedate
    - phone
    - sharewithfriendsasofdate
      
2. Step Trainer Records
This is the data from the motion sensor.
The file contains the following fields:
    - sensorReadingTime
    - serialNumber
    - distanceFromObject
      
3. Accelerometer Records
This is the data from the mobile app.
The file contains the following fields:
    - timeStamp
    - user
    - x
    - y
    - z

## Workflow
Using AWS Glue, AWS S3, Python, and Spark,  we will create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

Refer to the flowchart below to better understand the workflow.
![image](https://github.com/user-attachments/assets/5bbcd9fb-6d71-4fd3-9f09-75b796b11554)
A flowchart displaying the workflow.

## Data Engineering steps
1) Create the S3 buckets using the AWS S3 UI, the structure is as follows under the stedi s3 bucket:
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
2) Copy the data from github to s3 buckets
   git clone https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git stedi-project-starter-code
  aws s3 cp /home/cloudshell-user/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/customer/landing s3://stedi-datalake/customer/landing/ --recursive
  aws s3 cp /home/cloudshell-user/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing s3://stedi-datalake/accelerometer/landing/ --recursive
  aws s3 cp /home/cloudshell-user/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/ step_trainer/landing s3://stedi-datalake/ step_trainer/landing/ --recursive

3) Landing Zone --> Create a crawler using AWS glue to pick up the schema and do EDA on the data using AWS Athena
4) Trusted Zone --> a) Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.
                    b) Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.
                       This is done by doing an inner join between the customer_trusted data with the accelerometer_landing data by emails.
                       *Here we made sure to only pick records after the consent date of the user*
                    c)  step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated). This is done by doing an inner join between the step_trainer_landing data with the customer_curated data by serial numbers.
5) Curated Zone --> a) Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.
6)                     *Here we have anonymized the data so that it is not subject to GDPR*
7)                  b) Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.
                       machine_learning_curated.py has a node that inner joins the step_trainer_trusted data with the accelerometer_trusted data by sensor reading time and timestamps
Refer to the relationship diagram below to understand the desired state.
![image](https://github.com/user-attachments/assets/6ac15c2d-ffe1-417c-a346-829522248ec6)

## Challenges
