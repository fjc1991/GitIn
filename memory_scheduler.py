import os
import time
import threading
import queue
import psutil
from dataclasses import dataclass
from typing import Callable, Any, Dict, List, Optional
import traceback
import gc

# Set up logger for this module
from logger import get_logger
logger = get_logger(__name__)

@dataclass
class Job:
    """Data class representing a processing job."""
    id: str
    func: Callable
    args: tuple
    kwargs: Dict[str, Any]
    estimated_memory: int = 0  # Estimated memory in MB
    priority: int = 0  # Higher numbers = higher priority
    max_retries: int = 2
    retry_count: int = 0
    result: Any = None
    error: Optional[Exception] = None
    status: str = "pending"  # pending, running, completed, failed

class MemoryAwareScheduler:
    """
    Memory-aware job scheduler that controls the number of concurrent jobs
    based on available system memory.
    """
    def __init__(self, max_memory_percent=75, min_free_memory_mb=1000, max_workers=None):
        """
        Initialize the scheduler.
        
        Args:
            max_memory_percent (int): Maximum percentage of system memory to use
            min_free_memory_mb (int): Minimum free memory to maintain in MB
            max_workers (int): Maximum number of concurrent worker threads
        """
        self.max_memory_percent = max_memory_percent
        self.min_free_memory_mb = min_free_memory_mb
        self.max_workers = max_workers or os.cpu_count()
        
        # Job queue with priority
        self.job_queue = queue.PriorityQueue()
        
        # Active jobs
        self.active_jobs = {}
        self.active_jobs_lock = threading.Lock()
        
        # Completed/failed jobs
        self.completed_jobs = {}
        self.failed_jobs = {}
        
        # Control flags
        self.should_stop = False
        self.is_running = False
        
        # Worker threads
        self.workers = []
        self.scheduler_thread = None
    
    def submit(self, func, *args, estimated_memory=100, priority=0, job_id=None, **kwargs) -> str:
        """
        Submit a job to the scheduler.
        
        Args:
            func (callable): The function to execute
            *args: Positional arguments for the function
            estimated_memory (int): Estimated memory usage in MB
            priority (int): Job priority (higher = more important)
            job_id (str): Optional job ID, otherwise auto-generated
            **kwargs: Keyword arguments for the function
            
        Returns:
            str: Job ID
        """
        # Generate job ID if not provided
        if job_id is None:
            job_id = f"job_{time.time()}_{id(func)}"
        
        # Create job object
        job = Job(
            id=job_id,
            func=func,
            args=args,
            kwargs=kwargs,
            estimated_memory=estimated_memory,
            priority=priority
        )
        
        # Add to queue with priority (negative so higher numbers = higher priority)
        self.job_queue.put((-priority, job))
        logger.debug(f"Job {job_id} submitted (priority: {priority}, est. memory: {estimated_memory}MB)")
        
        return job_id
    
    def start(self):
        """Start the scheduler."""
        if self.is_running:
            return
        
        self.should_stop = False
        self.is_running = True
        
        # Start worker threads
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"worker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
        
        # Start scheduler thread
        self.scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            name="scheduler",
            daemon=True
        )
        self.scheduler_thread.start()
        
        logger.info(f"Memory-aware scheduler started with {self.max_workers} workers")
    
    def stop(self, wait=True):
        """
        Stop the scheduler.
        
        Args:
            wait (bool): Whether to wait for active jobs to complete
        """
        if not self.is_running:
            return
        
        logger.info("Stopping scheduler...")
        self.should_stop = True
        
        if wait:
            logger.info("Waiting for active jobs to complete...")
            # Wait for workers to finish
            for worker in self.workers:
                worker.join(timeout=60)
            
            if self.scheduler_thread:
                self.scheduler_thread.join(timeout=5)
        
        self.is_running = False
        logger.info("Scheduler stopped")
    
    def get_job_status(self, job_id):
        """Get the status of a job."""
        # Check active jobs
        with self.active_jobs_lock:
            if job_id in self.active_jobs:
                return {
                    "status": self.active_jobs[job_id].status,
                    "retry_count": self.active_jobs[job_id].retry_count
                }
        
        # Check completed jobs
        if job_id in self.completed_jobs:
            return {
                "status": "completed",
                "result": self.completed_jobs[job_id].result
            }
        
        # Check failed jobs
        if job_id in self.failed_jobs:
            return {
                "status": "failed",
                "error": str(self.failed_jobs[job_id].error),
                "retry_count": self.failed_jobs[job_id].retry_count
            }
        
        # Not found - might be in queue
        return {"status": "unknown"}
    
    def get_job_result(self, job_id, wait=False, timeout=None):
        """
        Get the result of a completed job.
        
        Args:
            job_id (str): The job ID
            wait (bool): Whether to wait for the job to complete
            timeout (float): Maximum time to wait in seconds
            
        Returns:
            The job result or None if not completed
        """
        start_time = time.time()
        
        while True:
            # Check if job is completed
            if job_id in self.completed_jobs:
                return self.completed_jobs[job_id].result
            
            # Check if job failed
            if job_id in self.failed_jobs:
                return None
            
            # If not waiting, return None
            if not wait:
                return None
            
            # Check timeout
            if timeout and time.time() - start_time > timeout:
                return None
            
            # Wait and check again
            time.sleep(0.5)
    
    def get_stats(self):
        """Get scheduler statistics."""
        return {
            "queue_size": self.job_queue.qsize(),
            "active_jobs": len(self.active_jobs),
            "completed_jobs": len(self.completed_jobs),
            "failed_jobs": len(self.failed_jobs),
            "memory_usage_percent": psutil.virtual_memory().percent
        }
    
    def _worker_loop(self):
        """Main worker loop."""
        while not self.should_stop:
            try:
                # Try to get a job from the queue with timeout
                try:
                    _, job = self.job_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # Mark job as running
                job.status = "running"
                
                # Add to active jobs
                with self.active_jobs_lock:
                    self.active_jobs[job.id] = job
                
                logger.debug(f"Starting job {job.id}")
                
                try:
                    # Execute the job
                    job.result = job.func(*job.args, **job.kwargs)
                    job.status = "completed"
                    
                    # Move to completed jobs
                    self.completed_jobs[job.id] = job
                    logger.debug(f"Job {job.id} completed successfully")
                
                except Exception as e:
                    job.error = e
                    job.status = "failed"
                    
                    # Check if should retry
                    if job.retry_count < job.max_retries:
                        job.retry_count += 1
                        job.status = "pending"
                        logger.warning(f"Job {job.id} failed, retrying ({job.retry_count}/{job.max_retries}): {str(e)}")
                        self.job_queue.put((-job.priority, job))
                    else:
                        # Move to failed jobs
                        self.failed_jobs[job.id] = job
                        logger.error(f"Job {job.id} failed after {job.retry_count} retries: {str(e)}")
                        logger.debug(traceback.format_exc())
                
                finally:
                    # Remove from active jobs
                    with self.active_jobs_lock:
                        if job.id in self.active_jobs:
                            del self.active_jobs[job.id]
                    
                    # Mark queue task as done
                    self.job_queue.task_done()
                    
                    # Force garbage collection after job
                    gc.collect()
            
            except Exception as e:
                logger.error(f"Error in worker thread: {str(e)}")
                logger.debug(traceback.format_exc())
    
    def _scheduler_loop(self):
        """Main scheduler loop for controlling resource usage."""
        while not self.is_running:
            # Wait for scheduler to be fully started
            time.sleep(0.1)
        
        while not self.should_stop:
            try:
                # Check system memory
                vm = psutil.virtual_memory()
                
                # Calculate available memory
                memory_available_mb = vm.available / (1024 * 1024)
                memory_usage_percent = vm.percent
                
                # Log memory stats periodically (every ~30 seconds)
                if int(time.time()) % 30 < 1:
                    logger.debug(f"Memory: {memory_usage_percent:.1f}% used, {memory_available_mb:.0f}MB available")
                    logger.debug(f"Active jobs: {len(self.active_jobs)}, Queue size: {self.job_queue.qsize()}")
                
                # Check if memory usage exceeds threshold
                if memory_usage_percent > self.max_memory_percent or memory_available_mb < self.min_free_memory_mb:
                    # Memory pressure detected
                    logger.warning(f"Memory pressure detected: {memory_usage_percent:.1f}% used, "
                                 f"{memory_available_mb:.0f}MB available")
                    
                    # Force garbage collection
                    gc.collect()
                    
                    # Pause until memory frees up
                    while (psutil.virtual_memory().percent > self.max_memory_percent - 5 or 
                           psutil.virtual_memory().available / (1024 * 1024) < self.min_free_memory_mb + 500):
                        logger.debug(f"Waiting for memory to free up. Current usage: {psutil.virtual_memory().percent:.1f}%")
                        time.sleep(5)
                
                # Sleep before next check
                time.sleep(2)
            
            except Exception as e:
                logger.error(f"Error in scheduler thread: {str(e)}")
                logger.debug(traceback.format_exc())
                time.sleep(5)  # Avoid tight loop on error

# Global scheduler instance
memory_scheduler = None

def get_scheduler(max_memory_percent=75, min_free_memory_mb=1000, max_workers=None):
    """Get or create the global scheduler instance."""
    global memory_scheduler
    
    if memory_scheduler is None:
        memory_scheduler = MemoryAwareScheduler(
            max_memory_percent=max_memory_percent,
            min_free_memory_mb=min_free_memory_mb,
            max_workers=max_workers
        )
    
    return memory_scheduler

def process_repos_with_scheduler(project_name, ecosystem, repos, start_date=None, end_date=None, 
                               temp_dir=None, output_dir=None, max_memory_percent=75, max_workers=None):
    """Process repositories using the memory-aware scheduler."""
    scheduler = get_scheduler(
        max_memory_percent=max_memory_percent,
        min_free_memory_mb=1000,
        max_workers=max_workers or max(1, os.cpu_count() - 1)
    )
    scheduler.start()
    
    from repo_processing import process_single_repo
    
    job_ids = []
    for i, repo in enumerate(repos):
        repo_url = repo['repo_url']
        repo_name = repo_url.split('/')[-1] if '/' in repo_url else f"repo_{i}"

        # Skip if output already exists
        output_pattern = f"{project_name}_{repo_name}_*_analysis"
        if output_dir:
            output_pattern = os.path.join(output_dir, output_pattern)

        # Use the helper function from analysis.py
        from analysis import check_output_exists
        existing_files = check_output_exists(output_dir, output_pattern)
        if existing_files:
            logger.info(f"Skipping already completed repo: {repo_name}")
            continue
        
        # Submit job to scheduler with estimated memory based on repo complexity
        job_id = scheduler.submit(
            process_single_repo,
            i, repo, project_name, ecosystem, repo['repo_category'],
            start_date, end_date, temp_dir, output_dir,
            estimated_memory=250,  # Base estimate in MB
            priority=i,  # Lower index = higher priority
            job_id=f"repo_{repo_name}"
        )
        job_ids.append(job_id)
    
    # Wait for all jobs to complete
    results = []
    for job_id in job_ids:
        result = scheduler.get_job_result(job_id, wait=True)
        if result:
            results.append(result)
    
    # Don't stop scheduler - let it be reused for other categories/projects
    
    return results