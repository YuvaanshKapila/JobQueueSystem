# JobQueueSystem
My attempt to make a somewhat functioning job queue system using docker, redis, and celery 

Just come docker config
<img width="1013" height="98" alt="Screenshot 2025-08-21 143722" src="https://github.com/user-attachments/assets/c471e55e-02b2-41a3-b25a-1a05e8bdcf43" />
<img width="586" height="135" alt="Screenshot 2025-08-21 143311" src="https://github.com/user-attachments/assets/a8d4bba7-8b19-4293-a6b9-5216b7e03c2c" />
<img width="615" height="191" alt="Screenshot 2025-08-21 142930" src="https://github.com/user-attachments/assets/af00330c-15da-470a-a103-2fd512d98bf7" />

<img width="846" height="1123" alt="image" src="https://github.com/user-attachments/assets/eb038213-6be3-4da9-a572-8d8c2b90063b" />



# Job Queue System (Celery + Redis + Docker)

## Overview
This project is a **task queue system** built with **Celery** and **Redis**, packaged in **Docker**.  

It allows scheduling and executing background jobs such as:
- Data processing  
- Report generation  
- Email dispatch  
- Security scans  
- System maintenance  
- Many more (see `main.py` for the full task list)  

The system supports **retries, progress tracking, and job persistence** (SQLite now, extendable to Postgres/MySQL later).  
A single **Celery Worker** processes tasks, and **Grafana** provides monitoring dashboards.  

---

## Requirements
- [Docker](https://www.docker.com/) & Docker Compose  
- Python 3.9+ (for local development outside Docker)  
- Redis (broker + metadata store)  

---

## Running the System
Clone the repository and start services with Docker:

```bash
git clone <your-repo-url>
cd <repo>
docker-compose up --build
This starts:

App (runs job manager + Celery setup)

Redis (broker and metadata)

Celery Worker (executes tasks)

Grafana (monitoring dashboards)

Usage
Submit tasks via the AdvancedJobManager:

python
Copy
Edit
from main import AdvancedJobManager, TaskType

manager = AdvancedJobManager()

# Example: submit a security scan
task_id = manager.submit_job(TaskType.SECURITY_SCAN, kwargs={
    "scan_type": "vulnerability",
    "scan_depth": "standard"
})

print("Submitted task:", task_id)
Check the status of a job:

python
Copy
Edit
job = manager.db.get_job(task_id)
print(job.status, job.result)
Monitoring
Grafana runs inside Docker and can be configured to pull metrics from Redis or Celery.

Dashboards show worker activity, task throughput, and error rates.

Current Limitations
Only one Celery worker is running (scaling possible).

No external DB yet (currently SQLite is used in main.py but not persisted in Docker).

Grafana dashboards need to be manually configured to track Redis/Celery metrics.

Roadmap
Add Postgres/MySQL for persistent job history.

Scale Celery workers with:

bash
Copy
Edit
docker-compose up --scale worker=3
Add Celery Beat for periodic scheduling (already partially configured in main.py).

Expand Grafana dashboards with job-level metrics.
