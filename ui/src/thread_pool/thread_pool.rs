use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;

type Job = Box<dyn FnOnce(usize) + Send + 'static>;

pub struct ThreadPool {
    _workers: Vec<Worker>,
    sender: Sender<Job>,
    task_cont: Arc<Mutex<usize>>,
    notification_receiver: Arc<Mutex<Receiver<()>>>,
    is_waiting: Arc<Mutex<bool>>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);
        // Create a channel to send jobs to the workers
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        // Create a channel to send notifications to the main thread when all the jobs are done
        let (notification_sender, notification_receiver) = mpsc::channel();
        let notification_sender = Arc::new(Mutex::new(notification_sender));
        let notification_receiver = Arc::new(Mutex::new(notification_receiver));

        // Create a counter to keep track of the number of jobs and is_waiting to know if the main thread is waiting
        let task_cont = Arc::new(Mutex::new(0));
        let is_waiting = Arc::new(Mutex::new(false)); 

        // Create the workers
        let mut _workers = Vec::with_capacity(size);
        for id in 0..size {
            _workers.push(Worker::new(
                id, 
                Arc::clone(&receiver), 
                Arc::clone(&task_cont),
                Arc::clone(&notification_sender),
                Arc::clone(&is_waiting),
            ));
        }

        ThreadPool {
            _workers,
            sender,
            task_cont,
            notification_receiver: Arc::clone(&notification_receiver),
            is_waiting: Arc::clone(&is_waiting),
        }
    }

    // Send a job to the worker
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce(usize) + Send + 'static,
    {
        // Send the job to the worker
        let job = Box::new(f);
        self.sender.send(job).unwrap();
    }
    
    // Wait until all the jobs are done
    pub fn wait(&self) {
        // If there are no jobs, return
        {
            let mut is_waiting = self.is_waiting.lock().unwrap();
            *is_waiting = true;
            let counter = self.task_cont.lock().unwrap();
            if *counter == 0 {
                *is_waiting = false;
                return;
            }
        }

        // Wait for a notification
        let receiver = self.notification_receiver.lock().unwrap();
        receiver.recv().unwrap();

        {
            let mut is_waiting = self.is_waiting.lock().unwrap();
            *is_waiting = false;
        }
    }
}

struct Worker {
    _id: usize,
    _thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>, task_cont: Arc<Mutex<usize>>, sender: Arc<Mutex<Sender<()>>>, is_waiting: Arc<Mutex<bool>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                // Get the job from the channel
                let job = receiver.lock().unwrap().recv().unwrap();
                {
                    // Increase the task counter
                    let mut counter = task_cont.lock().unwrap();
                    *counter += 1;
                }
                // Execute the job
                job(id);

                // Decrease the task counter
                let mut counter = task_cont.lock().unwrap();
                *counter -= 1;

                // If there are no more jobs and the main thread is waiting, send a notification
                if *counter == 0 {
                    let is_waiting = is_waiting.lock().unwrap();
                    if *is_waiting {
                        let notification_sender = sender.lock().unwrap();
                        notification_sender.send(()).unwrap();
                    }
                }
            }
        });

        Worker { _id: id, _thread: Some(thread) }
    }
}