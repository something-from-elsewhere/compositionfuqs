use std::any::Any;

use crate::tokens::Token;

pub fn assert_send_sync<T: Send + Sync>() {}

#[derive(Clone, Copy, Debug)]
pub struct Provenance {
    file_id: usize,
    line: usize,
    column: usize,
    size: usize,
}

#[derive(Debug, PartialEq)]
pub enum ModuleScopedId {
    Unresolved(String),
    Resolved(usize, usize),
}

#[derive(Debug, PartialEq)]
pub enum GlobalId {
    Unresolved(String),
    Resolved(usize),
}

#[derive(Debug)]
pub(crate) enum ConstMacro {
    Tokens(String, Vec<Token>),
}

#[derive(Debug)]
pub(crate) enum MacroFunc {
    Tokens(Vec<Token>, Vec<Token>),
}

pub(crate) fn unwrap_panic(payload: &Box<dyn Any + Send>) -> &str {
    if let Some(value) = payload.downcast_ref::<String>() {
        value.as_str()
    } else if let Some(value) = payload.downcast_ref::<&str>() {
        value
    } else {
        "Found a non-string panic payload!"
    }
}

impl Provenance {
    pub fn file_id(&self) -> usize {
        self.file_id
    }
    pub fn line(&self) -> usize {
        self.line
    }
    pub fn column(&self) -> usize {
        self.column
    }
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn new(file_id: usize, line: usize, column: usize, size: usize) -> Self {
        Self {
            file_id,
            line,
            column,
            size,
        }
    }
}
