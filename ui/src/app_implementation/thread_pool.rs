use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;

type Job = Box<dyn FnOnce(usize) + Send + 'static>;

pub struct ThreadPool {
    _workers: Vec<Worker>,
    sender: Sender<Job>,
    task_cont: Arc<Mutex<usize>>,
    notification_receiver: Arc<Mutex<Receiver<()>>>,
    waiting_flag: Arc<Mutex<bool>>,
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
            waiting_flag: Arc::clone(&is_waiting),
        }
    }

    // Send a job to the worker
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce(usize) + Send + 'static,
    {
        // Increase the task counter
        {
            let mut counter = self.task_cont.lock().unwrap();
            *counter += 1;
        }

        // Send the job to the worker
        let job = Box::new(f);
        self.sender.send(job).unwrap();
    }
    
    // Wait until all the jobs are done
    pub fn join(&self) {
        // If there are no jobs, return
        if *self.task_cont.lock().unwrap() == 0 {
            return;
        }
        self.set_waiting(true);
        self.notification_receiver.lock().unwrap().recv().unwrap();
        self.set_waiting(false);
    }

    pub fn set_waiting(&self, waiting: bool) {
        if let Ok(mut is_waiting) = self.waiting_flag.lock() {
            *is_waiting = waiting;
        }
    }
}

struct Worker {
    _id: usize,
    _thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>, task_cont: Arc<Mutex<usize>>, sender: Arc<Mutex<Sender<()>>>, waiting_flag: Arc<Mutex<bool>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                // Get and execute the job from the channel
                let job = receiver.lock().unwrap().recv().unwrap();
                job(id);

                // Decrease the task counter and check if there are no more jobs
                let no_more_jobes = {
                    let mut counter = task_cont.lock().unwrap();
                    *counter -= 1;
                    println!("Counter: {}", *counter);
                    *counter == 0
                };

                // If there are no more jobs and the main thread is waiting, send a notification
                if no_more_jobes && should_wait_for_completion(&waiting_flag) {
                    let notification_sender = sender.lock().unwrap();
                    notification_sender.send(()).unwrap();
                }
            }
        });

        Worker { _id: id, _thread: Some(thread) }
    }
}

fn should_wait_for_completion(waiting_flag: &Arc<Mutex<bool>>) -> bool {
    if let Ok(is_waiting) = waiting_flag.lock() {
        *is_waiting
    } else {
        false
    }
}