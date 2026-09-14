use std::{error::Error, fmt::Display, fs::File, mem};

use crate::lexer::LexResult;

#[derive(Debug)]
pub(crate) enum PartState {
    Processing,
    Source(File),
    Tokens(LexResult),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PartStage {
    Processing,
    Source,
    Tokens,
}

#[derive(Debug)]
pub(crate) struct Part {
    name: String,
    state: PartState,
}

#[derive(Debug)]
pub struct AlreadyProcessingError;

#[derive(Debug)]
pub struct NotProcessingError;

impl Part {
    pub(crate) fn new(source: File, name: String) -> Self {
        Self {
            state: PartState::Source(source),
            name,
        }
    }

    pub(crate) fn start_processing(&mut self) -> Result<PartState, AlreadyProcessingError> {
        if matches!(self.state, PartState::Processing) {
            return Err(AlreadyProcessingError);
        }

        Ok(mem::replace(&mut self.state, PartState::Processing))
    }

    pub(crate) fn finish_processing(&mut self, state: PartState) -> Result<(), NotProcessingError> {
        if matches!(self.state, PartState::Processing) {
            self.state = state;
            Ok(())
        } else {
            Err(NotProcessingError)
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name[..]
    }
}

impl Display for AlreadyProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cannot start processing a module if it's already being processed!"
        )
    }
}

impl Display for NotProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cannot finish processing if we never started!")
    }
}

impl From<&PartState> for PartStage {
    fn from(value: &PartState) -> Self {
        match value {
            PartState::Processing => PartStage::Processing,
            PartState::Source(_) => PartStage::Source,
            PartState::Tokens(_) => PartStage::Tokens,
        }
    }
}

impl Display for PartStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PartStage::Processing => write!(f, "Processing"),
            PartStage::Source => write!(f, "Source"),
            PartStage::Tokens => write!(f, "Tokens"),
        }
    }
}

impl Error for AlreadyProcessingError {}
impl Error for NotProcessingError {}
