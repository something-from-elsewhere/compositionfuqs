#![warn(clippy::pedantic)]
#![allow(dead_code)]
use std::path::Path;

use crate::compiler::Compiler;

mod common;
mod compiler;
mod lexer;
mod part;
mod scheduler;
mod tokens;

fn main() {
    let mut compiler = Compiler::new();
    let result = compiler.lex(Path::new("src/compfuqs/my.cfuq"), 8);
    match result {
        Ok(()) => (),
        Err(e) => eprintln!("{}", compiler.display_error(&e)),
    }
}
