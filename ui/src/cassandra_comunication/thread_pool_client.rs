use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;

use super::cassandra_client::CassandraClient;

type Job = Box<dyn FnOnce(usize, &CassandraClient) + Send + 'static>;
pub struct ThreadPoolClient {
    sender: Sender<Job>,
    task_cont: Arc<Mutex<usize>>,
    notification_receiver: Arc<Mutex<Receiver<()>>>,
    waiting_flag: Arc<Mutex<bool>>,
}

impl ThreadPoolClient {
    /// Create a new ThreadPoolClient with the given Cassandra clients
    pub fn new(clients: Vec<CassandraClient>) -> ThreadPoolClient {
        // Create a channel to send jobs to the workers
        let (sender, receiver) = Self::create_job_channel();

        // Create a channel to send notifications to the main thread when all the jobs are done
        let (notification_sender, notification_receiver) = Self::create_notification_channel();

        // Create a counter to keep track of the number of jobs and is_waiting to know if the main thread is waiting
        let task_cont = Arc::new(Mutex::new(0));
        let is_waiting = Arc::new(Mutex::new(false)); 

        // Create the workers
        let _workers = Self::create_workers(
            clients,
            Arc::clone(&receiver),
            Arc::clone(&task_cont),
            Arc::clone(&notification_sender),
            Arc::clone(&is_waiting),
        );

        ThreadPoolClient {
            sender,
            task_cont,
            notification_receiver: Arc::clone(&notification_receiver),
            waiting_flag: Arc::clone(&is_waiting),
        }
    }

    // Create a job channel (sender and shared receiver)
    fn create_job_channel() -> (Sender<Job>, Arc<Mutex<Receiver<Job>>>) {
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        (sender, receiver)
    }

    // Create a notification channel (sender and shared receiver)
    fn create_notification_channel() -> (
        Arc<Mutex<Sender<()>>>,
        Arc<Mutex<Receiver<()>>>,
    ) {
        let (notification_sender, notification_receiver) = mpsc::channel();
        let notification_sender = Arc::new(Mutex::new(notification_sender));
        let notification_receiver = Arc::new(Mutex::new(notification_receiver));
        (notification_sender, notification_receiver)
    }

    // Create a vector of workers for the thread pool
    fn create_workers(
        clients: Vec<CassandraClient>,
        receiver: Arc<Mutex<Receiver<Job>>>,
        task_cont: Arc<Mutex<usize>>,
        notification_sender: Arc<Mutex<Sender<()>>>,
        is_waiting: Arc<Mutex<bool>>,
    ) -> Vec<Worker> {
        clients
            .into_iter()
            .enumerate()
            .map(|(index, client)| {
                Worker::new(
                    index,
                    Arc::clone(&receiver),
                    Arc::clone(&task_cont),
                    Arc::clone(&notification_sender),
                    Arc::clone(&is_waiting),
                    client,
                )
            })
            .collect()
    }

    fn increase_counter_job(&self) {
        if let Ok(mut counter) = self.task_cont.lock() {
            *counter += 1;
        }
    }

    /// Send a job to the worker 
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce(usize, &CassandraClient) + Send + 'static,
    {
        self.increase_counter_job();
        let job = Box::new(f);
        let _ = self.sender.send(job);
    }

    fn no_more_jobes(&self) -> bool {
        if let Ok(counter) = self.task_cont.lock() {
            *counter == 0
        } else {
            false
        }
    }

    fn wait_notification(&self) {
        if let Ok(notification_receiver) = self.notification_receiver.lock() {
            let _ = notification_receiver.recv();
        }
    }
    
    /// Wait until all the jobs are done
    pub fn join(&self) {
        // If there are no jobs, return
        if self.no_more_jobes() {
            return;
        }
        self.set_waiting(true);
        self.wait_notification();
        self.set_waiting(false);
    }

    fn set_waiting(&self, waiting: bool) {
        if let Ok(mut is_waiting) = self.waiting_flag.lock() {
            *is_waiting = waiting;
        }
    }
}

struct Worker;

impl Worker {
    /// Create a new worker with the given id, receiver, task counter, sender, waiting flag and client
    /// The worker will execute the jobs from the receiver in a new thread
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Job>>>, task_cont: Arc<Mutex<usize>>, sender: Arc<Mutex<Sender<()>>>, waiting_flag: Arc<Mutex<bool>>, client: CassandraClient) -> Worker {
        let _thread = thread::spawn(move || {
            loop {
                // Get and execute the job from the channel
                let job = receiver.lock().unwrap().recv().unwrap();
                job(id, &client);

                // Decrease the task counter and check if there are no more jobs
                let task_decrease = decrease_counter_job(&task_cont);

                // If there are no more jobs and the main thread is waiting, send a notification
                if task_decrease == 0 && should_wait_for_completion(&waiting_flag) {
                    if let Ok(sender) = sender.lock() {
                        let _ = sender.send(());
                    }
                }
            }
        });
        Self
    }
}

fn should_wait_for_completion(waiting_flag: &Arc<Mutex<bool>>) -> bool {
    if let Ok(is_waiting) = waiting_flag.lock() {
        *is_waiting
    } else {
        false
    }
}

fn decrease_counter_job(task_cont: &Arc<Mutex<usize>>) -> usize {
    if let Ok(mut counter) = task_cont.lock() {
        *counter -= 1;
        return *counter;
    }
    1
}