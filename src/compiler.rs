use std::{
    error::Error,
    fmt::Display,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

use crate::{
    common::Provenance,
    compiler::CompilerError::Lex,
    lexer::{LexContext, LexError, Lexer},
    part::Part,
    scheduler::{Scheduler, SchedulerError},
};

#[derive(Debug)]
pub struct Compiler {
    pub(crate) parts: Arc<RwLock<Vec<(PathBuf, Part)>>>,
    errors: Vec<CompilerError>,
}

#[derive(Debug)]
pub enum CompilerError {
    Io(std::io::Error),
    Lex(LexError),
    PartCorrupted(PartCorruptedError),
    Scheduler(SchedulerError),
}

#[derive(Debug)]
pub struct CompilerErrorDisplay<'a> {
    compiler: &'a Compiler,
    error: &'a CompilerError,
}

#[derive(Debug)]
pub struct PartCorruptedError {
    name: String,
    id: usize,
}

impl Compiler {
    pub fn new() -> Self {
        Self {
            parts: Arc::new(RwLock::new(Vec::new())),
            errors: Vec::new(),
        }
    }

    /// # Parameters
    /// `root_file` is the path to the file to start compilation with, defaults to `src/main.cfuq` if empty
    ///
    /// `threads` is the number of threads to spawn for lexing. If 0 is passed, we infer a number from the environment
    pub fn lex(&mut self, root_file: &Path, threads: usize) -> Result<(), CompilerError> {
        let root_file = if root_file.is_empty() {
            Path::new("src/main.cfuq")
        } else {
            root_file
        };
        let name = root_file.to_str().unwrap_or("").replace(['\\', '/'], "::");
        let name = match name.strip_suffix(".cfuq") {
            Some(val) => val.to_string(),
            None => name,
        };
        let mut modules = self.parts.write().unwrap();
        modules.push((
            root_file.with_extension(""),
            Part::new(File::open(root_file.with_extension("cfuq"))?, name),
        ));
        drop(modules);
        let job_queue = Arc::new(Mutex::new(vec![0]));

        let ctx = LexContext::new(self.parts.clone(), job_queue.clone());
        let mut scheduler = Scheduler::<Lexer>::new(&mut self.errors, threads, job_queue, ctx);
        scheduler.run();
        // let mut idx = 0;
        // while idx < self.modules.len() {
        //     let module = self.modules[idx].1.start_processing();
        //     let module = match module {
        //         Ok(val) => val,
        //         Err(err) => {
        //             self.errors.push(err.into());
        //             continue;
        //         }
        //     };
        //     let file = match module {
        //         Source(val) => val,
        //         value => {
        //             return Err(WrongStageError {
        //                 stage: (&value).into(),
        //                 expected: ModuleStage::Source,
        //             }
        //             .into());
        //         }
        //     };
        //     let lxr = Lexer::new(&file, idx, self);
        //     let Ok(mut lxr) = lxr else {
        //         self.errors.push(
        //             ModuleCorruptedError {
        //                 name: self.modules[idx]
        //                     .0
        //                     .with_extension("cfuq")
        //                     .into_string()
        //                     .unwrap(),
        //                 id: idx,
        //             }
        //             .into(),
        //         );
        //         continue;
        //     };
        //     self.errors.extend(
        //         std::mem::take(&mut lxr.errors)
        //             .into_iter()
        //             .map(CompilerError::from),
        //     );
        //     self.modules[idx]
        //         .1
        //         .finish_processing(ModuleState::Tokens(lxr))?;
        //     idx += 1;
        // }

        Ok(())
    }

    pub fn display_error<'a>(&'a self, error: &'a CompilerError) -> CompilerErrorDisplay<'a> {
        CompilerErrorDisplay {
            compiler: self,
            error,
        }
    }
}

impl CompilerErrorDisplay<'_> {
    fn provenance(&self) -> Option<&Provenance> {
        match self.error {
            Lex(err) => err.provenance(),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CompilerError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<LexError> for CompilerError {
    fn from(value: LexError) -> Self {
        Self::Lex(value)
    }
}

impl From<PartCorruptedError> for CompilerError {
    fn from(value: PartCorruptedError) -> Self {
        Self::PartCorrupted(value)
    }
}

impl From<SchedulerError> for CompilerError {
    fn from(value: SchedulerError) -> Self {
        Self::Scheduler(value)
    }
}

impl Display for CompilerErrorDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(prov) = self.provenance() {
            let module = &self.compiler.parts.read().unwrap()[prov.file_id()];
            write!(
                f,
                "{}:{}:{}: {}",
                module.1.name(),
                prov.line(),
                prov.column(),
                self.error
            )
        } else {
            self.error.fmt(f)
        }
    }
}

impl Display for CompilerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => err.fmt(f),
            Self::Lex(err) => err.fmt(f),
            Self::PartCorrupted(err) => err.fmt(f),
            Self::Scheduler(err) => err.fmt(f),
        }
    }
}

impl Display for PartCorruptedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = &self.name[..];
        let id = self.id;
        write!(
            f,
            "Module #{id} (\"{name}.cfuq\") could not be loaded! Is the file corrupted?"
        )
    }
}

impl Error for CompilerError {}
impl Error for PartCorruptedError {}
