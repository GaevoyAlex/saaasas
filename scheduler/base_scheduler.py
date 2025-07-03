import asyncio
from typing import Callable, Any, Dict, List, Optional
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor
import structlog

logger = structlog.get_logger()

class BaseScheduler:
    def __init__(self):
        self.scheduler = None
        self.jobs = {}
        self.is_running = False
        self.logger = logger.bind(component="BaseScheduler")
        
        jobstores = {'default': MemoryJobStore()}
        executors = {'default': AsyncIOExecutor()}
        job_defaults = {'coalesce': False, 'max_instances': 1}
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults
        )
    
    def start(self):
        if not self.is_running:
            self.scheduler.start()
            self.is_running = True
            self.logger.info("Scheduler started")
    
    def shutdown(self, wait: bool = True):
        if self.is_running:
            self.scheduler.shutdown(wait=wait)
            self.is_running = False
            self.logger.info("Scheduler stopped")
    
    def add_interval_job(self, func: Callable, seconds: int, job_id: str, args: tuple = None, kwargs: dict = None):
        try:
            job = self.scheduler.add_job(
                func,
                trigger=IntervalTrigger(seconds=seconds),
                id=job_id,
                args=args or (),
                kwargs=kwargs or {},
                replace_existing=True
            )
            
            self.jobs[job_id] = job
            self.logger.info("Interval job added", job_id=job_id, seconds=seconds)
            return job
            
        except Exception as e:
            self.logger.error("Error adding interval job", job_id=job_id, error=str(e))
            raise
    
    def add_cron_job(self, func: Callable, cron_expression: str, job_id: str, args: tuple = None, kwargs: dict = None):
        try:
            cron_parts = cron_expression.split()
            if len(cron_parts) != 5:
                raise ValueError("Invalid cron expression")
            
            minute, hour, day, month, day_of_week = cron_parts
            
            job = self.scheduler.add_job(
                func,
                trigger=CronTrigger(
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week
                ),
                id=job_id,
                args=args or (),
                kwargs=kwargs or {},
                replace_existing=True
            )
            
            self.jobs[job_id] = job
            self.logger.info("Cron job added", job_id=job_id, cron=cron_expression)
            return job
            
        except Exception as e:
            self.logger.error("Error adding cron job", job_id=job_id, error=str(e))
            raise
    
    def remove_job(self, job_id: str):
        try:
            self.scheduler.remove_job(job_id)
            if job_id in self.jobs:
                del self.jobs[job_id]
            self.logger.info("Job removed", job_id=job_id)
        except Exception as e:
            self.logger.error("Error removing job", job_id=job_id, error=str(e))
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        jobs_info = []
        for job in self.scheduler.get_jobs():
            jobs_info.append({
                'id': job.id,
                'func': job.func.__name__,
                'trigger': str(job.trigger),
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None
            })
        return jobs_info