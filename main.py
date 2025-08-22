import os
import time
import random
import json
import uuid
import hashlib
import threading
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass, asdict
from contextlib import contextmanager
import signal
import sys
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
from queue import Queue, Empty
import pickle
import gzip
import base64
import math
from collections import defaultdict, deque
import heapq
import bisect

from celery import Celery, Task
from celery.result import AsyncResult
from celery.schedules import crontab
from celery.signals import task_prerun, task_postrun, task_failure, task_retry
from celery.exceptions import Retry, WorkerLostError
from kombu import Queue as KombuQueue
import redis
from flask import Flask, render_template, jsonify, request, Response, stream_template
from flask_cors import CORS
import requests
from werkzeug.serving import WSGIRequestHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_queue.log'),
        logging.StreamHandler()
    ]
)

class JobStatus(Enum):
    PENDING = "PENDING"
    RECEIVED = "RECEIVED"
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"

class Priority(Enum):
    LOWEST = 0
    LOW = 2
    NORMAL = 4
    HIGH = 6
    HIGHER = 8
    CRITICAL = 9

class TaskType(Enum):
    DATA_PROCESSING = "data_processing_task"
    EMAIL_NOTIFICATION = "email_task"
    REPORT_GENERATION = "report_generation_task"
    FILE_PROCESSING = "file_processing_task"
    DATABASE_OPERATION = "database_operation_task"
    API_INTEGRATION = "api_integration_task"
    IMAGE_PROCESSING = "image_processing_task"
    CLEANUP_OPERATION = "cleanup_task"
    HEALTH_CHECK = "health_check_task"
    BACKUP_OPERATION = "backup_task"
    ANALYTICS_COMPUTATION = "analytics_computation_task"
    NOTIFICATION_DISPATCH = "notification_dispatch_task"
    CACHE_OPERATION = "cache_operation_task"
    SECURITY_SCAN = "security_scan_task"
    SYSTEM_MAINTENANCE = "system_maintenance_task"

@dataclass
class JobMetrics:
    execution_count: int = 0
    total_execution_time: float = 0.0
    average_execution_time: float = 0.0
    success_rate: float = 0.0
    failure_count: int = 0
    retry_count: int = 0
    last_execution: Optional[datetime] = None
    memory_usage: float = 0.0
    cpu_usage: float = 0.0

@dataclass
class WorkerMetrics:
    worker_id: str
    active_tasks: int = 0
    processed_tasks: int = 0
    failed_tasks: int = 0
    uptime: float = 0.0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    last_heartbeat: Optional[datetime] = None

@dataclass
class QueueMetrics:
    queue_name: str
    pending_tasks: int = 0
    active_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    average_wait_time: float = 0.0
    throughput: float = 0.0

@dataclass
class JobResult:
    task_id: str
    task_name: str
    status: JobStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    traceback: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: float = 0.0
    retries: int = 0
    max_retries: int = 3
    priority: int = Priority.NORMAL.value
    queue_name: str = "default"
    worker_id: Optional[str] = None
    execution_time: float = 0.0
    memory_usage: float = 0.0
    cpu_usage: float = 0.0
    metadata: Dict[str, Any] = None

class DatabaseManager:
    def __init__(self, db_path: str = "job_queue.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        with self.get_connection() as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    task_id TEXT PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    traceback TEXT,
                    created_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    progress REAL DEFAULT 0.0,
                    retries INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    priority INTEGER DEFAULT 4,
                    queue_name TEXT DEFAULT 'default',
                    worker_id TEXT,
                    execution_time REAL DEFAULT 0.0,
                    memory_usage REAL DEFAULT 0.0,
                    cpu_usage REAL DEFAULT 0.0,
                    metadata TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS job_metrics (
                    task_name TEXT PRIMARY KEY,
                    execution_count INTEGER DEFAULT 0,
                    total_execution_time REAL DEFAULT 0.0,
                    average_execution_time REAL DEFAULT 0.0,
                    success_rate REAL DEFAULT 0.0,
                    failure_count INTEGER DEFAULT 0,
                    retry_count INTEGER DEFAULT 0,
                    last_execution TEXT,
                    memory_usage REAL DEFAULT 0.0,
                    cpu_usage REAL DEFAULT 0.0
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS worker_metrics (
                    worker_id TEXT PRIMARY KEY,
                    active_tasks INTEGER DEFAULT 0,
                    processed_tasks INTEGER DEFAULT 0,
                    failed_tasks INTEGER DEFAULT 0,
                    uptime REAL DEFAULT 0.0,
                    memory_usage REAL DEFAULT 0.0,
                    cpu_usage REAL DEFAULT 0.0,
                    last_heartbeat TEXT
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS queue_metrics (
                    queue_name TEXT PRIMARY KEY,
                    pending_tasks INTEGER DEFAULT 0,
                    active_tasks INTEGER DEFAULT 0,
                    completed_tasks INTEGER DEFAULT 0,
                    failed_tasks INTEGER DEFAULT 0,
                    average_wait_time REAL DEFAULT 0.0,
                    throughput REAL DEFAULT 0.0
                )
            ''')
            
            conn.commit()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def save_job(self, job: JobResult):
        with self.get_connection() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO jobs (
                    task_id, task_name, status, result, error, traceback,
                    created_at, started_at, completed_at, progress, retries,
                    max_retries, priority, queue_name, worker_id,
                    execution_time, memory_usage, cpu_usage, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job.task_id, job.task_name, job.status.value,
                json.dumps(job.result) if job.result else None,
                job.error, job.traceback,
                job.created_at.isoformat() if job.created_at else None,
                job.started_at.isoformat() if job.started_at else None,
                job.completed_at.isoformat() if job.completed_at else None,
                job.progress, job.retries, job.max_retries, job.priority,
                job.queue_name, job.worker_id, job.execution_time,
                job.memory_usage, job.cpu_usage,
                json.dumps(job.metadata) if job.metadata else None
            ))
            conn.commit()

    def get_job(self, task_id: str) -> Optional[JobResult]:
        with self.get_connection() as conn:
            row = conn.execute('SELECT * FROM jobs WHERE task_id = ?', (task_id,)).fetchone()
            if row:
                return JobResult(
                    task_id=row['task_id'],
                    task_name=row['task_name'],
                    status=JobStatus(row['status']),
                    result=json.loads(row['result']) if row['result'] else None,
                    error=row['error'],
                    traceback=row['traceback'],
                    created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
                    started_at=datetime.fromisoformat(row['started_at']) if row['started_at'] else None,
                    completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
                    progress=row['progress'],
                    retries=row['retries'],
                    max_retries=row['max_retries'],
                    priority=row['priority'],
                    queue_name=row['queue_name'],
                    worker_id=row['worker_id'],
                    execution_time=row['execution_time'],
                    memory_usage=row['memory_usage'],
                    cpu_usage=row['cpu_usage'],
                    metadata=json.loads(row['metadata']) if row['metadata'] else {}
                )
        return None

    def get_recent_jobs(self, limit: int = 100, status_filter: str = None) -> List[JobResult]:
        with self.get_connection() as conn:
            if status_filter:
                rows = conn.execute(
                    'SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT ?',
                    (status_filter, limit)
                ).fetchall()
            else:
                rows = conn.execute(
                    'SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?',
                    (limit,)
                ).fetchall()
            
            jobs = []
            for row in rows:
                jobs.append(JobResult(
                    task_id=row['task_id'],
                    task_name=row['task_name'],
                    status=JobStatus(row['status']),
                    result=json.loads(row['result']) if row['result'] else None,
                    error=row['error'],
                    traceback=row['traceback'],
                    created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
                    started_at=datetime.fromisoformat(row['started_at']) if row['started_at'] else None,
                    completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
                    progress=row['progress'],
                    retries=row['retries'],
                    max_retries=row['max_retries'],
                    priority=row['priority'],
                    queue_name=row['queue_name'],
                    worker_id=row['worker_id'],
                    execution_time=row['execution_time'],
                    memory_usage=row['memory_usage'],
                    cpu_usage=row['cpu_usage'],
                    metadata=json.loads(row['metadata']) if row['metadata'] else {}
                ))
            return jobs

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

celery_app = Celery(
    'job_queue_system',
    broker=redis_url,
    backend=redis_url,
    include=['tasks']
)

celery_app.conf.update(
    task_serializer='pickle',
    accept_content=['pickle', 'json'],
    result_serializer='pickle',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=4,
    task_acks_late=True,
    task_default_retry_delay=60,
    task_max_retries=5,
    worker_max_tasks_per_child=1000,
    worker_disable_rate_limits=False,
    task_compression='gzip',
    result_compression='gzip',
    task_always_eager=False,
    task_eager_propagates=True,
    task_store_eager_result=True,
    result_expires=3600 * 24 * 7,
    task_result_expires=3600 * 24 * 7,
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_routes={
        TaskType.DATA_PROCESSING.value: {'queue': 'data_processing', 'routing_key': 'data.processing'},
        TaskType.EMAIL_NOTIFICATION.value: {'queue': 'notifications', 'routing_key': 'notification.email'},
        TaskType.REPORT_GENERATION.value: {'queue': 'reports', 'routing_key': 'report.generation'},
        TaskType.FILE_PROCESSING.value: {'queue': 'file_processing', 'routing_key': 'file.processing'},
        TaskType.DATABASE_OPERATION.value: {'queue': 'database', 'routing_key': 'database.operation'},
        TaskType.API_INTEGRATION.value: {'queue': 'api_integration', 'routing_key': 'api.integration'},
        TaskType.IMAGE_PROCESSING.value: {'queue': 'image_processing', 'routing_key': 'image.processing'},
        TaskType.CLEANUP_OPERATION.value: {'queue': 'maintenance', 'routing_key': 'maintenance.cleanup'},
        TaskType.HEALTH_CHECK.value: {'queue': 'monitoring', 'routing_key': 'monitoring.health'},
        TaskType.BACKUP_OPERATION.value: {'queue': 'backup', 'routing_key': 'backup.operation'},
        TaskType.ANALYTICS_COMPUTATION.value: {'queue': 'analytics', 'routing_key': 'analytics.computation'},
        TaskType.NOTIFICATION_DISPATCH.value: {'queue': 'notifications', 'routing_key': 'notification.dispatch'},
        TaskType.CACHE_OPERATION.value: {'queue': 'cache', 'routing_key': 'cache.operation'},
        TaskType.SECURITY_SCAN.value: {'queue': 'security', 'routing_key': 'security.scan'},
        TaskType.SYSTEM_MAINTENANCE.value: {'queue': 'maintenance', 'routing_key': 'system.maintenance'},
    },
    task_default_queue='default',
    task_queues=(
        KombuQueue('data_processing', routing_key='data.processing'),
        KombuQueue('notifications', routing_key='notification.*'),
        KombuQueue('reports', routing_key='report.*'),
        KombuQueue('file_processing', routing_key='file.*'),
        KombuQueue('database', routing_key='database.*'),
        KombuQueue('api_integration', routing_key='api.*'),
        KombuQueue('image_processing', routing_key='image.*'),
        KombuQueue('maintenance', routing_key='maintenance.*'),
        KombuQueue('monitoring', routing_key='monitoring.*'),
        KombuQueue('backup', routing_key='backup.*'),
        KombuQueue('analytics', routing_key='analytics.*'),
        KombuQueue('cache', routing_key='cache.*'),
        KombuQueue('security', routing_key='security.*'),
        KombuQueue('default'),
    ),
    beat_schedule={
        'hourly-cleanup': {
            'task': TaskType.CLEANUP_OPERATION.value,
            'schedule': crontab(minute=0),
            'options': {'priority': Priority.LOW.value, 'queue': 'maintenance'}
        },
        'daily-backup': {
            'task': TaskType.BACKUP_OPERATION.value,
            'schedule': crontab(hour=2, minute=0),
            'options': {'priority': Priority.HIGH.value, 'queue': 'backup'}
        },
        'health-check-every-5min': {
            'task': TaskType.HEALTH_CHECK.value,
            'schedule': crontab(minute='*/5'),
            'options': {'priority': Priority.NORMAL.value, 'queue': 'monitoring'}
        },
        'weekly-report': {
            'task': TaskType.REPORT_GENERATION.value,
            'schedule': crontab(day_of_week=1, hour=6, minute=0),
            'args': ('weekly_summary',),
            'options': {'priority': Priority.HIGH.value, 'queue': 'reports'}
        },
        'daily-analytics': {
            'task': TaskType.ANALYTICS_COMPUTATION.value,
            'schedule': crontab(hour=3, minute=0),
            'args': ('daily_metrics',),
            'options': {'priority': Priority.NORMAL.value, 'queue': 'analytics'}
        },
        'security-scan-weekly': {
            'task': TaskType.SECURITY_SCAN.value,
            'schedule': crontab(day_of_week=0, hour=1, minute=0),
            'options': {'priority': Priority.HIGH.value, 'queue': 'security'}
        },
    }
)

redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
db_manager = DatabaseManager()

class BaseJobTask(Task):
    def on_success(self, retval, task_id, args, kwargs):
        job = db_manager.get_job(task_id)
        if job:
            job.status = JobStatus.SUCCESS
            job.result = retval
            job.completed_at = datetime.utcnow()
            if job.started_at:
                job.execution_time = (job.completed_at - job.started_at).total_seconds()
            db_manager.save_job(job)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        job = db_manager.get_job(task_id)
        if job:
            job.status = JobStatus.FAILURE
            job.error = str(exc)
            job.traceback = str(einfo)
            job.completed_at = datetime.utcnow()
            if job.started_at:
                job.execution_time = (job.completed_at - job.started_at).total_seconds()
            db_manager.save_job(job)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        job = db_manager.get_job(task_id)
        if job:
            job.status = JobStatus.RETRY
            job.retries += 1
            job.error = str(exc)
            db_manager.save_job(job)

def simulate_heavy_computation(duration_range=(1, 10), failure_rate=0.05, progress_updates=True):
    duration = random.uniform(*duration_range)
    steps = max(10, int(duration))
    step_duration = duration / steps
    
    for i in range(steps):
        time.sleep(step_duration)
        
        if random.random() < failure_rate:
            raise Exception(f"Simulated failure at step {i+1}/{steps}")
        
        if progress_updates and hasattr(simulate_heavy_computation, 'task_id'):
            progress = (i + 1) / steps * 100
            job = db_manager.get_job(simulate_heavy_computation.task_id)
            if job:
                job.progress = progress
                db_manager.save_job(job)
    
    return duration

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def data_processing_task(self, data_size, processing_type="standard", complexity=1.0):
    task_id = self.request.id
    simulate_heavy_computation.task_id = task_id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.DATA_PROCESSING.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='data_processing'
    )
    db_manager.save_job(job)
    
    try:
        processing_duration = {
            "light": (1, 3),
            "standard": (3, 8),
            "heavy": (8, 15),
            "intensive": (15, 30)
        }.get(processing_type, (3, 8))
        
        adjusted_duration = (
            processing_duration[0] * complexity,
            processing_duration[1] * complexity
        )
        
        execution_time = simulate_heavy_computation(adjusted_duration, failure_rate=0.08)
        
        processed_records = int(data_size * random.uniform(0.95, 1.05))
        output_size = processed_records * random.uniform(0.8, 1.3)
        
        result = {
            "task_type": "data_processing",
            "processing_type": processing_type,
            "complexity_factor": complexity,
            "input_size": data_size,
            "processed_records": processed_records,
            "output_size": output_size,
            "execution_time": execution_time,
            "throughput": processed_records / execution_time if execution_time > 0 else 0,
            "memory_peak": random.uniform(50, 200),
            "cpu_avg": random.uniform(40, 90),
            "success_rate": random.uniform(0.95, 1.0),
            "processed_at": datetime.utcnow().isoformat(),
            "batch_id": str(uuid.uuid4()),
            "checksum": hashlib.md5(str(processed_records).encode()).hexdigest()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1) * complexity)

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 4})
def email_task(self, recipient, subject, template="default", attachments=None, priority_level="normal"):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.EMAIL_NOTIFICATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='notifications'
    )
    db_manager.save_job(job)
    
    try:
        processing_time = {
            "low": random.uniform(1, 3),
            "normal": random.uniform(2, 5),
            "high": random.uniform(4, 8),
            "urgent": random.uniform(1, 2)
        }.get(priority_level, random.uniform(2, 5))
        
        time.sleep(processing_time)
        
        if random.random() < 0.03:
            raise Exception("SMTP server temporarily unavailable")
        
        if random.random() < 0.02:
            raise Exception("Recipient address validation failed")
        
        message_id = f"msg_{uuid.uuid4().hex[:12]}"
        delivery_time = random.uniform(0.1, 2.0)
        
        result = {
            "task_type": "email_notification",
            "recipient": recipient,
            "subject": subject,
            "template": template,
            "priority_level": priority_level,
            "message_id": message_id,
            "delivery_time": delivery_time,
            "processing_time": processing_time,
            "sent_at": datetime.utcnow().isoformat(),
            "attachment_count": len(attachments) if attachments else 0,
            "size_bytes": random.randint(1024, 102400),
            "smtp_response": "250 OK: Message accepted for delivery",
            "tracking_enabled": random.choice([True, False]),
            "encryption_used": random.choice(["TLS", "SSL", "None"]),
            "bounce_handling": "enabled",
            "spam_score": random.uniform(0.1, 2.0)
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def report_generation_task(self, report_type, parameters=None, format_type="pdf", include_charts=True):
    task_id = self.request.id
    simulate_heavy_computation.task_id = task_id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.REPORT_GENERATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='reports'
    )
    db_manager.save_job(job)
    
    try:
        report_complexity = {
            "summary": (5, 15),
            "detailed": (15, 30),
            "weekly_summary": (20, 40),
            "monthly_report": (30, 60),
            "annual_report": (60, 120),
            "custom": (10, 45)
        }.get(report_type, (10, 30))
        
        execution_time = simulate_heavy_computation(report_complexity, failure_rate=0.1)
        
        records_processed = random.randint(1000, 500000)
        file_size = random.randint(102400, 52428800)
        
        if random.random() < 0.05:
            raise Exception("Data source temporarily unavailable")
        
        if random.random() < 0.03:
            raise Exception("Report template rendering failed")
        
        result = {
            "task_type": "report_generation",
            "report_type": report_type,
            "format_type": format_type,
            "parameters": parameters or {},
            "include_charts": include_charts,
            "generated_at": datetime.utcnow().isoformat(),
            "execution_time": execution_time,
            "records_processed": records_processed,
            "file_size": file_size,
            "file_path": f"/reports/{report_type}_{int(time.time())}.{format_type}",
            "page_count": random.randint(5, 100),
            "chart_count": random.randint(3, 20) if include_charts else 0,
            "data_sources": random.randint(2, 10),
            "compression_ratio": random.uniform(0.3, 0.8),
            "quality_score": random.uniform(0.85, 1.0),
            "export_format": format_type.upper(),
            "metadata": {
                "author": "Job Queue System",
                "version": "1.0",
                "created_by": task_id
            }
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=120 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def file_processing_task(self, file_path, operation="process", output_format="json"):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.FILE_PROCESSING.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='file_processing'
    )
    db_manager.save_job(job)
    
    try:
        file_size = random.randint(1024, 1048576000)
        processing_time = max(1, file_size / 1000000)  
        time.sleep(min(processing_time, 30))
        
        if random.random() < 0.04:
            raise Exception("File corrupted or unreadable")
        
        operations_performed = []
        if operation in ["process", "all"]:
            operations_performed.extend(["validation", "parsing", "transformation"])
        if operation in ["convert", "all"]:
            operations_performed.append("format_conversion")
        if operation in ["analyze", "all"]:
            operations_performed.extend(["analysis", "indexing"])
        
        result = {
            "task_type": "file_processing",
            "file_path": file_path,
            "operation": operation,
            "output_format": output_format,
            "file_size": file_size,
            "processing_time": processing_time,
            "operations_performed": operations_performed,
            "lines_processed": random.randint(100, 1000000),
            "output_size": int(file_size * random.uniform(0.5, 1.5)),
            "validation_passed": random.choice([True, False]),
            "encoding_detected": random.choice(["utf-8", "ascii", "latin-1"]),
            "mime_type": random.choice(["text/plain", "text/csv", "application/json"]),
            "processed_at": datetime.utcnow().isoformat(),
            "checksum_original": hashlib.md5(file_path.encode()).hexdigest(),
            "checksum_processed": hashlib.md5(str(random.randint(1000, 9999)).encode()).hexdigest()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=45 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 4})
def database_operation_task(self, operation_type, table_name, query_params=None):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.DATABASE_OPERATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='database'
    )
    db_manager.save_job(job)
    
    try:
        operation_time = {
            "select": random.uniform(0.1, 2.0),
            "insert": random.uniform(0.5, 5.0),
            "update": random.uniform(1.0, 8.0),
            "delete": random.uniform(0.3, 3.0),
            "migrate": random.uniform(10.0, 60.0),
            "backup": random.uniform(5.0, 30.0),
            "index": random.uniform(2.0, 15.0)
        }.get(operation_type, random.uniform(1.0, 5.0))
        
        time.sleep(operation_time)
        
        if random.random() < 0.02:
            raise Exception("Database connection timeout")
        
        if random.random() < 0.01:
            raise Exception("Query execution failed - syntax error")
        
        records_affected = random.randint(1, 50000)
        
        result = {
            "task_type": "database_operation",
            "operation_type": operation_type,
            "table_name": table_name,
            "query_params": query_params or {},
            "records_affected": records_affected,
            "execution_time": operation_time,
            "connection_pool_size": random.randint(5, 20),
            "query_plan_cost": random.uniform(1.0, 100.0),
            "index_usage": random.choice(["full_scan", "index_seek", "index_scan"]),
            "lock_duration": random.uniform(0.001, 0.1),
            "cache_hit_ratio": random.uniform(0.7, 0.95),
            "transaction_id": str(uuid.uuid4()),
            "executed_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=30 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 6})
def api_integration_task(self, endpoint_url, method="GET", payload=None, headers=None, timeout=30):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.API_INTEGRATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='api_integration'
    )
    db_manager.save_job(job)
    
    try:
        request_time = random.uniform(0.5, 5.0)
        time.sleep(request_time)
        
        if random.random() < 0.05:
            raise Exception("API endpoint temporarily unavailable")
        
        if random.random() < 0.02:
            raise Exception("Authentication failed")
        
        status_codes = [200, 201, 202, 400, 401, 403, 404, 500, 502, 503]
        weights = [70, 10, 5, 3, 2, 1, 2, 3, 2, 2]
        status_code = random.choices(status_codes, weights=weights)[0]
        
        if status_code >= 400:
            raise Exception(f"API request failed with status {status_code}")
        
        response_size = random.randint(100, 10000)
        
        result = {
            "task_type": "api_integration",
            "endpoint_url": endpoint_url,
            "method": method,
            "status_code": status_code,
            "response_size": response_size,
            "request_time": request_time,
            "timeout": timeout,
            "headers_sent": len(headers) if headers else 0,
            "payload_size": len(str(payload)) if payload else 0,
            "rate_limit_remaining": random.randint(50, 1000),
            "cache_status": random.choice(["hit", "miss", "stale"]),
            "retry_after": None,
            "correlation_id": str(uuid.uuid4()),
            "api_version": random.choice(["v1", "v2", "v3"]),
            "requested_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=min(300, 60 * (self.request.retries + 1)))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def image_processing_task(self, image_path, operations=None, output_format="jpeg", quality=85):
    task_id = self.request.id
    simulate_heavy_computation.task_id = task_id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.IMAGE_PROCESSING.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='image_processing'
    )
    db_manager.save_job(job)
    
    try:
        operations = operations or ["resize", "optimize"]
        processing_time = len(operations) * random.uniform(2, 8)
        
        execution_time = simulate_heavy_computation((processing_time * 0.8, processing_time * 1.2), failure_rate=0.06)
        
        if random.random() < 0.03:
            raise Exception("Image file corrupted or unsupported format")
        
        original_size = random.randint(500000, 50000000)
        processed_size = int(original_size * random.uniform(0.3, 0.9))
        
        result = {
            "task_type": "image_processing",
            "image_path": image_path,
            "operations": operations,
            "output_format": output_format,
            "quality": quality,
            "original_size": original_size,
            "processed_size": processed_size,
            "compression_ratio": processed_size / original_size,
            "processing_time": execution_time,
            "dimensions_original": [random.randint(800, 4000), random.randint(600, 3000)],
            "dimensions_processed": [random.randint(400, 2000), random.randint(300, 1500)],
            "color_depth": random.choice([8, 16, 24, 32]),
            "metadata_preserved": random.choice([True, False]),
            "filters_applied": len(operations),
            "processed_at": datetime.utcnow().isoformat(),
            "output_path": f"/processed/{uuid.uuid4().hex}.{output_format}"
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask)
def cleanup_task(self, cleanup_type="routine", target_directories=None):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.CLEANUP_OPERATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='maintenance'
    )
    db_manager.save_job(job)
    
    try:
        cleanup_time = {
            "routine": random.uniform(2, 8),
            "deep": random.uniform(10, 30),
            "emergency": random.uniform(1, 5)
        }.get(cleanup_type, random.uniform(5, 15))
        
        time.sleep(cleanup_time)
        
        files_deleted = random.randint(10, 1000)
        space_freed = random.randint(1048576, 1073741824)  
        directories_cleaned = random.randint(3, 50)
        
        result = {
            "task_type": "cleanup_operation",
            "cleanup_type": cleanup_type,
            "target_directories": target_directories or ["/tmp", "/cache", "/logs"],
            "files_deleted": files_deleted,
            "space_freed_bytes": space_freed,
            "space_freed_mb": round(space_freed / 1048576, 2),
            "directories_cleaned": directories_cleaned,
            "cleanup_time": cleanup_time,
            "oldest_file_age_days": random.randint(1, 365),
            "largest_file_size": random.randint(1048576, 104857600),
            "cleanup_criteria": ["age > 30 days", "size > 100MB", "temp files"],
            "errors_encountered": random.randint(0, 5),
            "cleaned_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        return {"error": str(exc), "cleanup_type": cleanup_type}

@celery_app.task(bind=True, base=BaseJobTask)
def health_check_task(self, components=None):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.HEALTH_CHECK.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='monitoring'
    )
    db_manager.save_job(job)
    
    try:
        check_time = random.uniform(1, 4)
        time.sleep(check_time)
        
        components = components or ["database", "redis", "filesystem", "memory", "cpu"]
        
        component_status = {}
        overall_healthy = True
        
        for component in components:
            is_healthy = random.random() > 0.05  
            response_time = random.uniform(0.001, 0.5)
            
            component_status[component] = {
                "healthy": is_healthy,
                "response_time": response_time,
                "last_check": datetime.utcnow().isoformat(),
                "details": {
                    "uptime": random.uniform(3600, 604800),
                    "usage_percent": random.uniform(10, 95)
                }
            }
            
            if not is_healthy:
                overall_healthy = False
        
        result = {
            "task_type": "health_check",
            "overall_status": "healthy" if overall_healthy else "unhealthy",
            "components": component_status,
            "check_duration": check_time,
            "checked_at": datetime.utcnow().isoformat(),
            "system_metrics": {
                "memory_usage_percent": random.uniform(30, 85),
                "cpu_usage_percent": random.uniform(10, 70),
                "disk_usage_percent": random.uniform(20, 90),
                "network_latency_ms": random.uniform(1, 50),
                "active_connections": random.randint(10, 500)
            },
            "alerts": [] if overall_healthy else ["Component failure detected"],
            "next_check_scheduled": (datetime.utcnow() + timedelta(minutes=5)).isoformat()
        }
        
        return result
        
    except Exception as exc:
        return {"error": str(exc), "status": "check_failed"}

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 2})
def backup_task(self, backup_type="incremental", targets=None, compression=True):
    task_id = self.request.id
    simulate_heavy_computation.task_id = task_id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.BACKUP_OPERATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='backup'
    )
    db_manager.save_job(job)
    
    try:
        backup_duration = {
            "incremental": (5, 15),
            "differential": (10, 25),
            "full": (30, 120)
        }.get(backup_type, (10, 30))
        
        execution_time = simulate_heavy_computation(backup_duration, failure_rate=0.03)
        
        targets = targets or ["database", "application_files", "user_data"]
        data_size = random.randint(1073741824, 107374182400)  
        compressed_size = int(data_size * random.uniform(0.3, 0.8)) if compression else data_size
        
        result = {
            "task_type": "backup_operation",
            "backup_type": backup_type,
            "targets": targets,
            "compression_enabled": compression,
            "original_size_bytes": data_size,
            "compressed_size_bytes": compressed_size,
            "compression_ratio": compressed_size / data_size if compression else 1.0,
            "execution_time": execution_time,
            "backup_location": f"/backups/{backup_type}_{int(time.time())}.tar.gz",
            "files_backed_up": random.randint(1000, 100000),
            "verification_passed": random.choice([True, False]),
            "backup_speed_mbps": (data_size / 1048576) / execution_time if execution_time > 0 else 0,
            "encryption_used": random.choice(["AES-256", "RSA-2048", "None"]),
            "retention_days": random.choice([7, 30, 90, 365]),
            "created_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=180 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def analytics_computation_task(self, analysis_type, dataset_params=None, time_range=None):
    task_id = self.request.id
    simulate_heavy_computation.task_id = task_id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.ANALYTICS_COMPUTATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='analytics'
    )
    db_manager.save_job(job)
    
    try:
        computation_complexity = {
            "simple": (2, 8),
            "moderate": (8, 20),
            "complex": (20, 45),
            "daily_metrics": (10, 25),
            "trend_analysis": (15, 35),
            "predictive_model": (30, 90)
        }.get(analysis_type, (10, 25))
        
        execution_time = simulate_heavy_computation(computation_complexity, failure_rate=0.04)
        
        records_analyzed = random.randint(10000, 10000000)
        
        if random.random() < 0.02:
            raise Exception("Insufficient data for reliable analysis")
        
        result = {
            "task_type": "analytics_computation",
            "analysis_type": analysis_type,
            "dataset_params": dataset_params or {},
            "time_range": time_range or {"start": "2024-01-01", "end": "2024-12-31"},
            "records_analyzed": records_analyzed,
            "execution_time": execution_time,
            "processing_rate": records_analyzed / execution_time if execution_time > 0 else 0,
            "insights_generated": random.randint(5, 50),
            "confidence_score": random.uniform(0.7, 0.95),
            "statistical_significance": random.uniform(0.01, 0.05),
            "anomalies_detected": random.randint(0, 10),
            "correlation_coefficients": [random.uniform(-1, 1) for _ in range(5)],
            "model_accuracy": random.uniform(0.8, 0.98),
            "feature_importance": {f"feature_{i}": random.uniform(0.1, 1.0) for i in range(10)},
            "computed_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=90 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5})
def notification_dispatch_task(self, notification_type, recipients, message_content, channels=None):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.NOTIFICATION_DISPATCH.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='notifications'
    )
    db_manager.save_job(job)
    
    try:
        channels = channels or ["email", "sms", "push"]
        dispatch_time = len(recipients) * len(channels) * random.uniform(0.1, 0.5)
        time.sleep(min(dispatch_time, 30))
        
        if random.random() < 0.03:
            raise Exception("Notification service rate limit exceeded")
        
        delivery_results = {}
        total_sent = 0
        total_failed = 0
        
        for channel in channels:
            sent = random.randint(int(len(recipients) * 0.8), len(recipients))
            failed = len(recipients) - sent
            total_sent += sent
            total_failed += failed
            
            delivery_results[channel] = {
                "sent": sent,
                "failed": failed,
                "delivery_rate": sent / len(recipients) if recipients else 0,
                "avg_delivery_time": random.uniform(0.5, 3.0)
            }
        
        result = {
            "task_type": "notification_dispatch",
            "notification_type": notification_type,
            "recipient_count": len(recipients),
            "channels": channels,
            "total_sent": total_sent,
            "total_failed": total_failed,
            "overall_success_rate": total_sent / (total_sent + total_failed) if (total_sent + total_failed) > 0 else 0,
            "delivery_results": delivery_results,
            "dispatch_time": dispatch_time,
            "message_size": len(str(message_content)),
            "priority": random.choice(["low", "normal", "high", "urgent"]),
            "retry_policy": "exponential_backoff",
            "dispatched_at": datetime.utcnow().isoformat(),
            "batch_id": str(uuid.uuid4())
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=45 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 4})
def cache_operation_task(self, operation, cache_key=None, cache_value=None, ttl=3600):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.CACHE_OPERATION.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='cache'
    )
    db_manager.save_job(job)
    
    try:
        operation_time = {
            "set": random.uniform(0.001, 0.01),
            "get": random.uniform(0.001, 0.005),
            "delete": random.uniform(0.001, 0.01),
            "flush": random.uniform(0.1, 1.0),
            "invalidate": random.uniform(0.01, 0.1)
        }.get(operation, random.uniform(0.001, 0.01))
        
        time.sleep(operation_time)
        
        if random.random() < 0.01:
            raise Exception("Cache server connection failed")
        
        cache_stats = {
            "hit_rate": random.uniform(0.7, 0.95),
            "miss_rate": random.uniform(0.05, 0.3),
            "eviction_rate": random.uniform(0.01, 0.1),
            "memory_usage": random.uniform(0.3, 0.9),
            "key_count": random.randint(1000, 1000000)
        }
        
        result = {
            "task_type": "cache_operation",
            "operation": operation,
            "cache_key": cache_key,
            "ttl": ttl,
            "operation_time": operation_time,
            "cache_hit": random.choice([True, False]) if operation == "get" else None,
            "data_size": len(str(cache_value)) if cache_value else 0,
            "cache_stats": cache_stats,
            "server_node": random.choice(["cache-1", "cache-2", "cache-3"]),
            "compression_used": random.choice([True, False]),
            "serialization_format": random.choice(["json", "pickle", "msgpack"]),
            "executed_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=15 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask, autoretry_for=(Exception,), retry_kwargs={'max_retries': 3})
def security_scan_task(self, scan_type="vulnerability", targets=None, scan_depth="standard"):
    task_id = self.request.id
    simulate_heavy_computation.task_id = task_id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.SECURITY_SCAN.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='security'
    )
    db_manager.save_job(job)
    
    try:
        scan_duration = {
            "quick": (2, 8),
            "standard": (10, 25),
            "deep": (30, 90),
            "comprehensive": (60, 180)
        }.get(scan_depth, (10, 25))
        
        execution_time = simulate_heavy_computation(scan_duration, failure_rate=0.02)
        
        targets = targets or ["web_application", "database", "network", "filesystem"]
        
        vulnerabilities_found = random.randint(0, 20)
        severity_distribution = {
            "critical": random.randint(0, 3),
            "high": random.randint(0, 5),
            "medium": random.randint(0, 8),
            "low": random.randint(0, 15)
        }
        
        result = {
            "task_type": "security_scan",
            "scan_type": scan_type,
            "scan_depth": scan_depth,
            "targets": targets,
            "execution_time": execution_time,
            "vulnerabilities_found": vulnerabilities_found,
            "severity_distribution": severity_distribution,
            "security_score": random.uniform(60, 95),
            "compliance_status": random.choice(["compliant", "non_compliant", "partial"]),
            "ports_scanned": random.randint(100, 65535),
            "services_identified": random.randint(5, 50),
            "false_positives": random.randint(0, 5),
            "recommendations": random.randint(3, 15),
            "scan_coverage": random.uniform(0.8, 1.0),
            "baseline_comparison": random.choice(["improved", "degraded", "unchanged"]),
            "scanned_at": datetime.utcnow().isoformat(),
            "next_scan_recommended": (datetime.utcnow() + timedelta(days=7)).isoformat()
        }
        
        return result
        
    except Exception as exc:
        raise self.retry(exc=exc, countdown=120 * (self.request.retries + 1))

@celery_app.task(bind=True, base=BaseJobTask)
def system_maintenance_task(self, maintenance_type="routine", components=None, scheduled_downtime=False):
    task_id = self.request.id
    
    job = JobResult(
        task_id=task_id,
        task_name=TaskType.SYSTEM_MAINTENANCE.value,
        status=JobStatus.STARTED,
        created_at=datetime.utcnow(),
        started_at=datetime.utcnow(),
        queue_name='maintenance'
    )
    db_manager.save_job(job)
    
    try:
        maintenance_duration = {
            "routine": random.uniform(5, 15),
            "preventive": random.uniform(15, 45),
            "emergency": random.uniform(2, 10),
            "scheduled": random.uniform(30, 120)
        }.get(maintenance_type, random.uniform(10, 30))
        
        time.sleep(maintenance_duration)
        
        components = components or ["database", "cache", "logs", "temp_files", "indexes"]
        
        maintenance_results = {}
        for component in components:
            maintenance_results[component] = {
                "status": random.choice(["completed", "skipped", "failed"]),
                "duration": random.uniform(1, 10),
                "issues_found": random.randint(0, 5),
                "issues_resolved": random.randint(0, 5),
                "performance_impact": random.uniform(0, 0.1)
            }
        
        result = {
            "task_type": "system_maintenance",
            "maintenance_type": maintenance_type,
            "scheduled_downtime": scheduled_downtime,
            "components": components,
            "maintenance_duration": maintenance_duration,
            "maintenance_results": maintenance_results,
            "system_health_before": random.uniform(0.7, 0.9),
            "system_health_after": random.uniform(0.85, 1.0),
            "performance_improvement": random.uniform(0.05, 0.25),
            "resources_freed": random.randint(100, 10000),
            "alerts_cleared": random.randint(0, 10),
            "next_maintenance": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "maintenance_window": f"{datetime.utcnow().isoformat()} - {(datetime.utcnow() + timedelta(seconds=maintenance_duration)).isoformat()}",
            "completed_at": datetime.utcnow().isoformat()
        }
        
        return result
        
    except Exception as exc:
        return {"error": str(exc), "maintenance_type": maintenance_type, "partial_completion": True}

class AdvancedJobManager:
    def __init__(self):
        self.celery = celery_app
        self.redis = redis_client
        self.db = db_manager
        self.task_registry = {
            TaskType.DATA_PROCESSING: data_processing_task,
            TaskType.EMAIL_NOTIFICATION: email_task,
            TaskType.REPORT_GENERATION: report_generation_task,
            TaskType.FILE_PROCESSING: file_processing_task,
            TaskType.DATABASE_OPERATION: database_operation_task,
            TaskType.API_INTEGRATION: api_integration_task,
            TaskType.IMAGE_PROCESSING: image_processing_task,
            TaskType.CLEANUP_OPERATION: cleanup_task,
            TaskType.HEALTH_CHECK: health_check_task,
            TaskType.BACKUP_OPERATION: backup_task,
            TaskType.ANALYTICS_COMPUTATION: analytics_computation_task,
            TaskType.NOTIFICATION_DISPATCH: notification_dispatch_task,
            TaskType.CACHE_OPERATION: cache_operation_task,
            TaskType.SECURITY_SCAN: security_scan_task,
            TaskType.SYSTEM_MAINTENANCE: system_maintenance_task
        }
        
    def submit_job(self, task_type: Union[TaskType, str], args: tuple = (), kwargs: dict = None,
                   priority: Priority = Priority.NORMAL, delay: int = 0, 
                   eta: datetime = None, retry_policy: dict = None) -> str:
        kwargs = kwargs or {}
        
        if isinstance(task_type, str):
            task_name = task_type
        else:
            task_name = task_type.value
        
        options = {
            'priority': priority.value,
            'retry_policy': retry_policy or {'max_retries': 3, 'interval_start': 60}
        }
        
        if delay > 0:
            options['countdown'] = delay
        elif eta:
            options['eta'] = eta
        
        result = self.celery.send_task(task_name, args=args, kwargs=kwargs, **options)
        
        job = JobResult(
            task_id=result.id,
            task_name=task_name,
            status=JobStatus.PENDING,
            created_at=datetime.utcnow(),
            priority=priority.value,
            queue_name=self.get_queue_for_task(task_name),
            metadata=kwargs
        )
        self.db.save_job(job)
        
        self.redis.hset(f"task:{result.id}", mapping={
            "task_name": task_name,
            "created_at": datetime.utcnow().isoformat(),
            "status": "PENDING",
            "priority": priority.value,
            "args": json.dumps(args),
            "kwargs": json.dumps(kwargs)
        })
        
        return result.id
    
    def get_queue_for_task(self, task_name: str) -> str:
        queue_mapping = {
            TaskType.DATA_PROCESSING.value: 'data_processing',
            TaskType.EMAIL_NOTIFICATION.value: 'notifications',
            TaskType.REPORT_GENERATION.value: 'reports',
            TaskType.FILE_PROCESSING.value: 'file_processing',
            TaskType.DATABASE_OPERATION.value: 'database',
            TaskType.API_INTEGRATION.value: 'api_integration',
            TaskType.IMAGE_PROCESSING.value: 'image_processing',
            TaskType.CLEANUP_OPERATION.value: 'maintenance',
            TaskType.HEALTH_CHECK.value: 'monitoring',
            TaskType.BACKUP_OPERATION.value: 'backup',
            TaskType.ANALYTICS_COMPUTATION.value: 'analytics',
            TaskType.NOTIFICATION_DISPATCH.value: 'notifications',
            TaskType.CACHE_OPERATION.value: 'cache',
            TaskType.SECURITY_SCAN.value: 'security',
            TaskType.SYSTEM_MAINTENANCE.value: 'maintenance'
        }
        return queue_mapping.get(task_name, 'default')
    
    def get_job_status(self, task_id: str) -> Optional[JobResult]:
        job = self.db.get_job(task_id)
        if job:
            result = AsyncResult(task_id, app=self.celery)
            job.status = JobStatus(result.status)
            if result.successful():
                job.result = result.result
            elif result.failed():
                job.error = str(result.result)
                job.traceback = result.traceback
        return job
    
    def cancel_job(self, task_id: str) -> bool:
        try:
            self.celery.control.revoke(task_id, terminate=True)
            job = self.db.get_job(task_id)
            if job:
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.utcnow()
                self.db.save_job(job)
            self.redis.hset(f"task:{task_id}", "status", "CANCELLED")
            return True
        except Exception as e:
            logging.error(f"Failed to cancel job {task_id}: {e}")
            return False
    
    def get_queue_stats(self) -> Dict[str, Any]:
        inspect = self.celery.control.inspect()
        
        try:
            active_tasks = inspect.active() or {}
            scheduled_tasks = inspect.scheduled() or {}
            reserved_tasks = inspect.reserved() or {}
            registered_tasks = inspect.registered() or {}
            stats = inspect.stats() or {}
        except Exception as e:
            logging.error(f"Failed to get celery stats: {e}")
            active_tasks = scheduled_tasks = reserved_tasks = registered_tasks = stats = {}
        
        queue_stats = {
            "active_tasks": sum(len(tasks) for tasks in active_tasks.values()),
            "scheduled_tasks": sum(len(tasks) for tasks in scheduled_tasks.values()),
            "reserved_tasks": sum(len(tasks) for tasks in reserved_tasks.values()),
            "total_workers": len(active_tasks),
            "workers": [],
            "queues": {},
            "system_stats": {}
        }
        
        for worker, tasks in active_tasks.items():
            worker_info = {
                "name": worker,
                "active_tasks": len(tasks),
                "status": "online",
                "load_avg": stats.get(worker, {}).get('rusage', {}).get('utime', 0.0),
                "memory_usage": random.uniform(100, 500),  
                "uptime": random.uniform(3600, 86400)
            }
            queue_stats["workers"].append(worker_info)
        
        queue_names = ['data_processing', 'notifications', 'reports', 'file_processing', 
                      'database', 'api_integration', 'image_processing', 'maintenance', 
                      'monitoring', 'backup', 'analytics', 'cache', 'security', 'default']
        
        for queue_name in queue_names:
            try:
                queue_length = self.redis.llen(queue_name) or 0
            except:
                queue_length = 0
                
            queue_stats["queues"][queue_name] = {
                "pending": queue_length,
                "active": sum(1 for tasks in active_tasks.values() 
                            for task in tasks if task.get('delivery_info', {}).get('routing_key', '').startswith(queue_name)),
                "throughput": random.uniform(0.1, 10.0),
                "avg_processing_time": random.uniform(1.0, 30.0)
            }
        
        try:
            redis_info = self.redis.info()
            queue_stats["system_stats"] = {
                "redis_memory": redis_info.get('used_memory_human', 'N/A'),
                "redis_uptime": redis_info.get('uptime_in_seconds', 0),
                "redis_connected_clients": redis_info.get('connected_clients', 0),
                "total_commands_processed": redis_info.get('total_commands_processed', 0)
            }
        except Exception as e:
            logging.error(f"Failed to get Redis stats: {e}")
            queue_stats["system_stats"] = {}
        
        return queue_stats
    
    def get_recent_jobs(self, limit: int = 100, status_filter: str = None, 
                       task_type_filter: str = None) -> List[JobResult]:
        return self.db.get_recent_jobs(limit, status_filter)
    
    def get_job_metrics(self, task_name: str) -> Optional[JobMetrics]:
        try:
            with self.db.get_connection() as conn:
                row = conn.execute(
                    'SELECT * FROM job_metrics WHERE task_name = ?', 
                    (task_name,)
                ).fetchone()
                
                if row:
                    return JobMetrics(
                        execution_count=row['execution_count'],
                        total_execution_time=row['total_execution_time'],
                        average_execution_time=row['average_execution_time'],
                        success_rate=row['success_rate'],
                        failure_count=row['failure_count'],
                        retry_count=row['retry_count'],
                        last_execution=datetime.fromisoformat(row['last_execution']) if row['last_execution'] else None,
                        memory_usage=row['memory_usage'],
                        cpu_usage=row['cpu_usage']
                    )
        except Exception as e:
            logging.error(f"Failed to get job metrics for {task_name}: {e}")
        return None
    
    def update_job_metrics(self, task_name: str, execution_time: float, 
                          success: bool, memory_usage: float = 0.0, cpu_usage: float = 0.0):
        try:
            with self.db.get_connection() as conn:
                existing = conn.execute(
                    'SELECT * FROM job_metrics WHERE task_name = ?', 
                    (task_name,)
                ).fetchone()
                
                if existing:
                    new_count = existing['execution_count'] + 1
                    new_total_time = existing['total_execution_time'] + execution_time
                    new_avg_time = new_total_time / new_count
                    new_failure_count = existing['failure_count'] + (0 if success else 1)
                    new_success_rate = (new_count - new_failure_count) / new_count
                    
                    conn.execute('''
                        UPDATE job_metrics 
                        SET execution_count = ?, total_execution_time = ?, 
                            average_execution_time = ?, success_rate = ?,
                            failure_count = ?, last_execution = ?,
                            memory_usage = ?, cpu_usage = ?
                        WHERE task_name = ?
                    ''', (new_count, new_total_time, new_avg_time, new_success_rate,
                         new_failure_count, datetime.utcnow().isoformat(),
                         memory_usage, cpu_usage, task_name))
                else:
                    conn.execute('''
                        INSERT INTO job_metrics 
                        (task_name, execution_count, total_execution_time, 
                         average_execution_time, success_rate, failure_count,
                         last_execution, memory_usage, cpu_usage)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (task_name, 1, execution_time, execution_time,
                         1.0 if success else 0.0, 0 if success else 1,
                         datetime.utcnow().isoformat(), memory_usage, cpu_usage))
                
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to update job metrics for {task_name}: {e}")
    
    def get_queue_metrics(self) -> List[QueueMetrics]:
        queue_names = ['data_processing', 'notifications', 'reports', 'file_processing', 
                      'database', 'api_integration', 'image_processing', 'maintenance', 
                      'monitoring', 'backup', 'analytics', 'cache', 'security', 'default']
        
        metrics = []
        for queue_name in queue_names:
            try:
                with self.db.get_connection() as conn:
                    row = conn.execute(
                        'SELECT * FROM queue_metrics WHERE queue_name = ?',
                        (queue_name,)
                    ).fetchone()
                    
                    if row:
                        metrics.append(QueueMetrics(
                            queue_name=row['queue_name'],
                            pending_tasks=row['pending_tasks'],
                            active_tasks=row['active_tasks'],
                            completed_tasks=row['completed_tasks'],
                            failed_tasks=row['failed_tasks'],
                            average_wait_time=row['average_wait_time'],
                            throughput=row['throughput']
                        ))
                    else:
                        metrics.append(QueueMetrics(queue_name=queue_name))
            except Exception as e:
                logging.error(f"Failed to get metrics for queue {queue_name}: {e}")
                metrics.append(QueueMetrics(queue_name=queue_name))
        
        return metrics
    
    def bulk_submit_jobs(self, jobs: List[Dict[str, Any]]) -> List[str]:
        task_ids = []
        for job_def in jobs:
            try:
                task_id = self.submit_job(
                    task_type=job_def.get('task_type'),
                    args=job_def.get('args', ()),
                    kwargs=job_def.get('kwargs', {}),
                    priority=Priority(job_def.get('priority', Priority.NORMAL.value)),
                    delay=job_def.get('delay', 0)
                )
                task_ids.append(task_id)
            except Exception as e:
                logging.error(f"Failed to submit bulk job: {e}")
                task_ids.append(None)
        
        return task_ids
    
    def get_job_history(self, task_type: str = None, days: int = 7) -> List[JobResult]:
        try:
            with self.db.get_connection() as conn:
                cutoff_date = (datetime.utcnow() - timedelta(days=days)).isoformat()
                
                if task_type:
                    rows = conn.execute('''
                        SELECT * FROM jobs 
                        WHERE task_name = ? AND created_at >= ?
                        ORDER BY created_at DESC
                    ''', (task_type, cutoff_date)).fetchall()
                else:
                    rows = conn.execute('''
                        SELECT * FROM jobs 
                        WHERE created_at >= ?
                        ORDER BY created_at DESC
                    ''', (cutoff_date,)).fetchall()
                
                jobs = []
                for row in rows:
                    jobs.append(JobResult(
                        task_id=row['task_id'],
                        task_name=row['task_name'],
                        status=JobStatus(row['status']),
                        result=json.loads(row['result']) if row['result'] else None,
                        error=row['error'],
                        traceback=row['traceback'],
                        created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
                        started_at=datetime.fromisoformat(row['started_at']) if row['started_at'] else None,
                        completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
                        progress=row['progress'],
                        retries=row['retries'],
                        max_retries=row['max_retries'],
                        priority=row['priority'],
                        queue_name=row['queue_name'],
                        worker_id=row['worker_id'],
                        execution_time=row['execution_time'],
                        memory_usage=row['memory_usage'],
                        cpu_usage=row['cpu_usage'],
                        metadata=json.loads(row['metadata']) if row['metadata'] else {}
                    ))
                
                return jobs
        except Exception as e:
            logging.error(f"Failed to get job history: {e}")
            return []

flask_app = Flask(__name__)
CORS(flask_app)

flask_app.config.update(
    SECRET_KEY=os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production'),
    DEBUG=os.getenv('DEBUG', 'False').lower() == 'true',
    TESTING=False,
    JSON_SORT_KEYS=False
)

job_manager = AdvancedJobManager()

@flask_app.route('/')
def dashboard():
    return render_template('dashboard.html')

@flask_app.route('/api/health')
def health_check():
    try:
        redis_status = redis_client.ping()
        celery_status = True  
        
        return jsonify({
            'status': 'healthy',
            'services': {
                'redis': 'up' if redis_status else 'down',
                'celery': 'up' if celery_status else 'down',
                'database': 'up'
            },
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@flask_app.route('/api/stats')
def get_stats():
    try:
        stats = job_manager.get_queue_stats()
        return jsonify(stats)
    except Exception as e:
        logging.error(f"Failed to get stats: {e}")
        return jsonify({'error': 'Failed to retrieve statistics'}), 500

@flask_app.route('/api/jobs')
def get_jobs():
    try:
        limit = request.args.get('limit', 100, type=int)
        status_filter = request.args.get('status')
        task_type_filter = request.args.get('task_type')
        
        jobs = job_manager.get_recent_jobs(limit, status_filter, task_type_filter)
        
        jobs_data = []
        for job in jobs:
            job_data = {
                'task_id': job.task_id,
                'task_name': job.task_name,
                'status': job.status.value,
                'result': job.result,
                'error': job.error,
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'started_at': job.started_at.isoformat() if job.started_at else None,
                'completed_at': job.completed_at.isoformat() if job.completed_at else None,
                'progress': job.progress,
                'retries': job.retries,
                'max_retries': job.max_retries,
                'priority': job.priority,
                'queue_name': job.queue_name,
                'worker_id': job.worker_id,
                'execution_time': job.execution_time,
                'memory_usage': job.memory_usage,
                'cpu_usage': job.cpu_usage
            }
            jobs_data.append(job_data)
        
        return jsonify(jobs_data)
    except Exception as e:
        logging.error(f"Failed to get jobs: {e}")
        return jsonify({'error': 'Failed to retrieve jobs'}), 500

@flask_app.route('/api/job/<task_id>')
def get_job(task_id):
    try:
        job = job_manager.get_job_status(task_id)
        if not job:
            return jsonify({'error': 'Job not found'}), 404
        
        return jsonify({
            'task_id': job.task_id,
            'task_name': job.task_name,
            'status': job.status.value,
            'result': job.result,
            'error': job.error,
            'traceback': job.traceback,
            'created_at': job.created_at.isoformat() if job.created_at else None,
            'started_at': job.started_at.isoformat() if job.started_at else None,
            'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            'progress': job.progress,
            'retries': job.retries,
            'max_retries': job.max_retries,
            'priority': job.priority,
            'queue_name': job.queue_name,
            'worker_id': job.worker_id,
            'execution_time': job.execution_time,
            'memory_usage': job.memory_usage,
            'cpu_usage': job.cpu_usage,
            'metadata': job.metadata
        })
    except Exception as e:
        logging.error(f"Failed to get job {task_id}: {e}")
        return jsonify({'error': 'Failed to retrieve job'}), 500

@flask_app.route('/api/job/submit', methods=['POST'])
def submit_job():
    try:
        data = request.json
        
        if not data or not data.get('task_name'):
            return jsonify({'error': 'Missing task_name'}), 400
        
        task_name = data.get('task_name')
        args = data.get('args', [])
        kwargs = data.get('kwargs', {})
        priority = Priority(data.get('priority', Priority.NORMAL.value))
        delay = data.get('delay', 0)
        
        task_id = job_manager.submit_job(
            task_type=task_name,
            args=tuple(args),
            kwargs=kwargs,
            priority=priority,
            delay=delay
        )
        
        return jsonify({
            'task_id': task_id,
            'status': 'submitted',
            'message': f'Job {task_id} submitted successfully'
        })
    except ValueError as e:
        return jsonify({'error': f'Invalid parameter: {str(e)}'}), 400
    except Exception as e:
        logging.error(f"Failed to submit job: {e}")
        return jsonify({'error': 'Failed to submit job'}), 500

@flask_app.route('/api/job/bulk_submit', methods=['POST'])
def bulk_submit_jobs():
    try:
        data = request.json
        
        if not data or not isinstance(data.get('jobs'), list):
            return jsonify({'error': 'Missing jobs array'}), 400
        
        jobs = data.get('jobs', [])
        task_ids = job_manager.bulk_submit_jobs(jobs)
        
        return jsonify({
            'task_ids': task_ids,
            'submitted_count': len([tid for tid in task_ids if tid is not None]),
            'failed_count': len([tid for tid in task_ids if tid is None])
        })
    except Exception as e:
        logging.error(f"Failed to bulk submit jobs: {e}")
        return jsonify({'error': 'Failed to bulk submit jobs'}), 500

@flask_app.route('/api/job/<task_id>/cancel', methods=['POST'])
def cancel_job(task_id):
    try:
        success = job_manager.cancel_job(task_id)
        if success:
            return jsonify({
                'success': True,
                'message': f'Job {task_id} cancelled successfully'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to cancel job'
            }), 500
    except Exception as e:
        logging.error(f"Failed to cancel job {task_id}: {e}")
        return jsonify({'error': 'Failed to cancel job'}), 500

@flask_app.route('/api/metrics/jobs')
def get_job_metrics():
    try:
        task_name = request.args.get('task_name')
        if task_name:
            metrics = job_manager.get_job_metrics(task_name)
            if metrics:
                return jsonify(asdict(metrics))
            else:
                return jsonify({'error': 'Metrics not found'}), 404
        else:
            all_task_types = [task_type.value for task_type in TaskType]
            all_metrics = {}
            for task_type in all_task_types:
                metrics = job_manager.get_job_metrics(task_type)
                if metrics:
                    all_metrics[task_type] = asdict(metrics)
            return jsonify(all_metrics)
    except Exception as e:
        logging.error(f"Failed to get job metrics: {e}")
        return jsonify({'error': 'Failed to retrieve job metrics'}), 500

@flask_app.route('/api/metrics/queues')
def get_queue_metrics():
    try:
        metrics = job_manager.get_queue_metrics()
        return jsonify([asdict(metric) for metric in metrics])
    except Exception as e:
        logging.error(f"Failed to get queue metrics: {e}")
        return jsonify({'error': 'Failed to retrieve queue metrics'}), 500

@flask_app.route('/api/history')
def get_job_history():
    try:
        task_type = request.args.get('task_type')
        days = request.args.get('days', 7, type=int)
        
        history = job_manager.get_job_history(task_type, days)
        
        history_data = []
        for job in history:
            job_data = {
                'task_id': job.task_id,
                'task_name': job.task_name,
                'status': job.status.value,
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'execution_time': job.execution_time,
                'retries': job.retries,
                'priority': job.priority,
                'queue_name': job.queue_name
            }
            history_data.append(job_data)
        
        return jsonify(history_data)
    except Exception as e:
        logging.error(f"Failed to get job history: {e}")
        return jsonify({'error': 'Failed to retrieve job history'}), 500

@flask_app.route('/api/tasks/types')
def get_task_types():
    try:
        task_types = []
        for task_type in TaskType:
            task_info = {
                'name': task_type.value,
                'display_name': task_type.name.replace('_', ' ').title(),
                'queue': job_manager.get_queue_for_task(task_type.value),
                'description': f"Handles {task_type.name.replace('_', ' ').lower()} operations"
            }
            task_types.append(task_info)
        
        return jsonify(task_types)
    except Exception as e:
        logging.error(f"Failed to get task types: {e}")
        return jsonify({'error': 'Failed to retrieve task types'}), 500

@flask_app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@flask_app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("Starting Job Queue System...")
    print(f"Dashboard will be available at: http://localhost:5000")
    print(f"API endpoints available at: http://localhost:5000/api/*")
    print(f"Redis URL: {redis_url}")
    print(f"Database: {db_manager.db_path}")
    
    flask_app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,
        threaded=True,
        use_reloader=False
    )
