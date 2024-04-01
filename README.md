**1. Setting up Authentication**  
_**Navigation >> IAM and admin >> Service accounts**_  
Creating a Service account within GCP with a BigQuery User role.  
Service account: my-project-1-393913-60c0ae7f9a40.json  
Dataset: my-project-1-393913.test_task  


**2. Creating Google Cloud Function**  
_**Navigation >> Cloud Function >> Create Function**_  
Creating Google Cloud Function for data pipeline  
- Trigger type: HTTP
- Tick "Require authentication"
- Saving provided URL for further steps
- Runtime environment: Python 3.12
- Source code: Inline editor
  

**3. Creating Cloud Scheduler**  
_**Navigation >> Cloud Scheduler >> Create Job**_  
Creating Cloud Scheduler for daily data ingestion    
- Target type: HTTP
- URL: pasting URL from previous step
- HTTP method: GET
- Frequency: 0 0 * * *
