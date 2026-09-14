use std::{
    error::Error,
    fmt, mem,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError, RwLockWriteGuard,
        mpsc::{self, Receiver, RecvError, Sender},
    },
    thread::{self, JoinHandle, available_parallelism},
};

use crate::{
    common::unwrap_panic,
    compiler::CompilerError,
    lexer::LexError,
    part::{AlreadyProcessingError, NotProcessingError, Part, PartStage},
    scheduler::SchedulerResponse::NewJob,
};

pub(crate) struct Scheduler<'a, S>
where
    S: Stage,
{
    errors: &'a mut Vec<CompilerError>,
    threads: usize,
    job_queue: Arc<Mutex<Vec<usize>>>,
    workers: Vec<Worker<S>>,
    ctx: S::Context,
    producer: Sender<(usize, WorkerRequest<S>)>,
    consumer: Receiver<(usize, WorkerRequest<S>)>,
}

pub(crate) trait Stage
where
    Self: Sized,
    Self: 'static,
    Self: Send,
{
    type Result: Send + 'static;
    type Job: Send + 'static;
    type Request: Send + 'static;
    type Response: Send + 'static;
    type Context: Send + 'static;

    fn new(
        id: usize,
        rx: Receiver<SchedulerResponse<Self>>,
        tx: Sender<(usize, WorkerRequest<Self>)>,
    ) -> Self;

    fn spin_up(self) -> Result<(), SchedulerError>;

    fn handle_request(rq: Self::Request, ctx: &mut Self::Context) -> Self::Response;

    fn new_job(ctx: &mut Self::Context) -> Result<Option<Self::Job>, SchedulerError>;

    fn commit_work(result: Self::Result, ctx: &mut Self::Context) -> Result<(), SchedulerError>;
}

pub(crate) enum WorkerRequest<S: Stage> {
    NewJob(S::Result),
    Request(S::Request),
}

pub(crate) enum SchedulerResponse<S: Stage> {
    NewJob(usize, S::Job),
    Respond(S::Response),
    ShutDown,
}

enum WorkerState {
    Idle,
    Running,
    Dead,
    Retired,
}

struct Worker<S: Stage> {
    state: WorkerState,
    handle: JoinHandle<Result<(), SchedulerError>>,
    tx: Sender<SchedulerResponse<S>>,
}

#[derive(Debug)]
pub enum SchedulerError {
    Lex(LexError),
    ThreadPanic(ThreadPanicError),
    FailedResponse(FailedResponseError),
    FailedRequest(FailedRequestError),
    WrongStage(WrongStageError),
    AlreadyProcessing(AlreadyProcessingError),
    NotProcessing(NotProcessingError),
    Poisoned(PoisonedError),
    Recv(RecvError),
}

#[derive(Debug)]
pub struct FailedResponseError {
    worker_id: usize,
    msg_type: &'static str,
}

#[derive(Debug)]
pub struct FailedRequestError {
    worker_id: usize,
    msg_type: &'static str,
}

#[derive(Debug)]
pub struct ThreadPanicError {
    thread_id: usize,
    message: String,
}

#[derive(Debug)]
pub struct WrongStageError {
    stage: PartStage,
    expected: PartStage,
}

#[derive(Debug)]
pub struct PoisonedError {
    who: &'static str,
}

impl<'a, S: Stage> Scheduler<'a, S> {
    pub(crate) fn new(
        errors: &'a mut Vec<CompilerError>,
        threads: usize,
        job_queue: Arc<Mutex<Vec<usize>>>,
        ctx: S::Context,
    ) -> Self {
        // Prepare to delegate tasks...
        let threads = if threads != 0 {
            threads
        } else {
            available_parallelism().unwrap().get()
        };
        let mut workers = Vec::new();
        let (producer, consumer) = mpsc::channel();
        // Spin up the thread pool...
        for idx in 0..threads {
            let (tx, rx) = mpsc::channel();
            let producer = producer.clone();
            workers.push(Worker {
                state: WorkerState::Idle,
                handle: thread::spawn(move || S::new(idx, rx, producer).spin_up()),
                tx,
            });
        }
        Self {
            errors,
            threads,
            job_queue,
            workers,
            ctx,
            producer,
            consumer,
        }
    }

    pub(crate) fn run(&mut self) {
        loop {
            if self.update_worker_state() {
                break;
            }

            for (worker_id, request) in self.consumer.try_iter() {
                match request {
                    WorkerRequest::NewJob(result) => {
                        self.workers[worker_id].state = WorkerState::Idle;
                        if let Err(err) = S::commit_work(result, &mut self.ctx) {
                            self.errors.push(err.into());
                        }
                    }
                    WorkerRequest::Request(request) => {
                        if self.workers[worker_id]
                            .tx
                            .send(SchedulerResponse::Respond(S::handle_request(
                                request,
                                &mut self.ctx,
                            )))
                            .is_err()
                        {
                            self.errors.push(
                                SchedulerError::from(FailedResponseError {
                                    worker_id,
                                    msg_type: "Response to Request",
                                })
                                .into(),
                            );
                        }
                    }
                }
            }
        }
    }

    /// # Returns
    /// `true` if all workers are idle, and no new tasks could be assigned
    fn update_worker_state(&mut self) -> bool {
        let mut all_workers_idle = true;
        for (i, worker) in self.workers.iter_mut().enumerate() {
            if worker.handle.is_finished() {
                worker.state = WorkerState::Dead;
            }
            match worker.state {
                WorkerState::Idle => {
                    let jobs = self.job_queue.lock().unwrap();
                    if jobs.is_empty() {
                        continue;
                    }
                    let job_id = jobs[0];
                    drop(jobs);
                    let job = match S::new_job(&mut self.ctx) {
                        Ok(value) => value,
                        Err(error) => {
                            self.errors.push(error.into());
                            None
                        }
                    };
                    if let Some(job) = job {
                        if worker.tx.send(NewJob(job_id, job)).is_err() {
                            self.errors.push(
                                SchedulerError::from(FailedResponseError {
                                    worker_id: i,
                                    msg_type: "New Job",
                                })
                                .into(),
                            );
                        } else {
                            worker.state = WorkerState::Running;
                            all_workers_idle = false;
                        }
                    }
                }
                WorkerState::Dead => {
                    all_workers_idle = false;
                    let (tx, rx) = mpsc::channel();
                    let producer = self.producer.clone();
                    let dead_worker = mem::replace(
                        worker,
                        Worker {
                            state: WorkerState::Idle,
                            handle: thread::spawn(move || S::new(i, rx, producer).spin_up()),
                            tx,
                        },
                    );
                    match dead_worker.handle.join() {
                        Err(error) => {
                            self.errors.push(
                                SchedulerError::from(ThreadPanicError {
                                    thread_id: i,
                                    message: unwrap_panic(&error).to_string(),
                                })
                                .into(),
                            );
                        }
                        Ok(Err(error)) => self.errors.push(error.into()),

                        Ok(Ok(())) => worker.state = WorkerState::Retired,
                    }
                }
                WorkerState::Running => all_workers_idle = false,
                WorkerState::Retired => (),
            }
        }
        all_workers_idle
    }
}

impl WrongStageError {
    pub(crate) fn new(stage: PartStage, expected: PartStage) -> Self {
        Self { stage, expected }
    }
}

impl FailedRequestError {
    pub(crate) fn new(worker_id: usize, msg_type: &'static str) -> Self {
        Self {
            worker_id,
            msg_type,
        }
    }
}

impl fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lex(err) => err.fmt(f),
            Self::ThreadPanic(err) => err.fmt(f),
            Self::FailedResponse(err) => err.fmt(f),
            Self::FailedRequest(err) => err.fmt(f),
            Self::Recv(err) => err.fmt(f),
            Self::WrongStage(err) => err.fmt(f),
            Self::Poisoned(err) => err.fmt(f),
            Self::AlreadyProcessing(err) => err.fmt(f),
            Self::NotProcessing(err) => err.fmt(f),
        }
    }
}

impl From<LexError> for SchedulerError {
    fn from(err: LexError) -> Self {
        Self::Lex(err)
    }
}

impl From<AlreadyProcessingError> for SchedulerError {
    fn from(err: AlreadyProcessingError) -> Self {
        Self::AlreadyProcessing(err)
    }
}

impl From<NotProcessingError> for SchedulerError {
    fn from(err: NotProcessingError) -> Self {
        Self::NotProcessing(err)
    }
}

impl From<PoisonError<MutexGuard<'_, Vec<usize>>>> for SchedulerError {
    fn from(_: PoisonError<MutexGuard<'_, Vec<usize>>>) -> Self {
        Self::Poisoned(PoisonedError { who: "Job Queue" })
    }
}

impl From<PoisonError<RwLockWriteGuard<'_, Vec<(PathBuf, Part)>>>> for SchedulerError {
    fn from(_: PoisonError<RwLockWriteGuard<'_, Vec<(PathBuf, Part)>>>) -> Self {
        Self::Poisoned(PoisonedError { who: "Modules" })
    }
}

impl From<ThreadPanicError> for SchedulerError {
    fn from(err: ThreadPanicError) -> Self {
        Self::ThreadPanic(err)
    }
}

impl From<RecvError> for SchedulerError {
    fn from(err: RecvError) -> Self {
        Self::Recv(err)
    }
}

impl From<FailedResponseError> for SchedulerError {
    fn from(err: FailedResponseError) -> Self {
        Self::FailedResponse(err)
    }
}

impl From<FailedRequestError> for SchedulerError {
    fn from(value: FailedRequestError) -> Self {
        Self::FailedRequest(value)
    }
}

impl From<WrongStageError> for SchedulerError {
    fn from(err: WrongStageError) -> Self {
        Self::WrongStage(err)
    }
}

impl fmt::Display for ThreadPanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.thread_id;
        let msg = self.message.as_str();
        write!(f, "Worker {id} panicked with message \"{msg}\"!")
    }
}

impl fmt::Display for FailedResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.worker_id;
        let msg = self.msg_type;
        write!(f, "Failed to send \"{msg}\" response to worker {id}!")
    }
}

impl fmt::Display for FailedRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = self.worker_id;
        let msg_type = self.msg_type;
        write!(f, "Worker {id} failed to send \"{msg_type}\" request!")
    }
}

impl fmt::Display for WrongStageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let expected = self.expected;
        let stage = self.stage;
        write!(
            f,
            "Expected stage \"{expected}\", but module was in stage \"{stage}\"!"
        )
    }
}

impl fmt::Display for PoisonedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let who = self.who;
        write!(f, "\"{who}\" was poisoned!")
    }
}

impl Error for SchedulerError {}
impl Error for ThreadPanicError {}
impl Error for FailedResponseError {}
impl Error for FailedRequestError {}
