from multiprocessing  import Process, Queue, Value, cpu_count, Event
import os
import math
import traceback


class Worker(Process):
    def __init__(self, jobs_queue, complete_count):
        super(Worker, self).__init__();
        self.jobs_queue = jobs_queue;
        self.complete_count = complete_count;
        self.exit = Event();
        return;

    def job_completed(self):
        self.complete_count.value += 1;
        return

    def run(self):
        for job in iter( self.jobs_queue.get, None):
            #Generic Function to be called before all processes
            try :
                job.process();
            except Exception as e:
                # Log Exception Here
                print traceback.print_exc();
            self.job_completed();
        return;

    def shutown(self):
        self.exit.set();

class ProcessManager:
    def __init__(self, process_count = None):
        # get CPU count for number of process count
        if process_count == None:
            process_count = cpu_count();
        self.max_worker_count = process_count;
        self.jobs_queue = Queue();
        self.jobs_added_count = 0;
        self.jobs_completed_count = Value('i', 0);
        self.worker_queue = [];
        # Creating workers
        self.create_workers();
        return;

    def create_workers(self):
        for counter in range(0, self.max_worker_count):
            self.worker_queue.append(Worker(self.jobs_queue,
                                            self.jobs_completed_count));
        return;

    def start_workers(self):
        for worker in self.worker_queue:
            worker.start();
        return;
    
    def add_jobs(self, job):
        self.jobs_queue.put(job);
        self.jobs_added_count += 1;
        return;

    def are_all_jobs_completed(self):
        return self.jobs_added_count == self.jobs_completed_count.value;
    
    def jobs_in_queue(self):
        return self.jobs_added_count.qsize();
    
    def close(self):
        for worker in self.worker_queue:
            worker.terminate();
        for worker in self.worker_queue:
            worker.join();
        return;
