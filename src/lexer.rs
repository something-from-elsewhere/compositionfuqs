use crate::common::{ConstMacro, MacroFunc, ModuleScopedId, Provenance};
use crate::part::{AlreadyProcessingError, Part, PartStage, PartState};
use crate::scheduler::{
    FailedRequestError, SchedulerError, SchedulerResponse, Stage, WorkerRequest, WrongStageError,
};
use crate::tokens::TokenKind::{self, Delim, Keyword, Literal, MacroInv, Operator};
use crate::tokens::{self, DelimKind, Token, TokenKind::Directive};
use crate::tokens::{
    DELIMS, DIRECTIVES, KEYWORDS, LiteralKind, MULTI_DELIMS, MULTI_OPS, OPS, OperatorKind,
};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::{fmt, mem};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};
use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug)]
pub struct LexResult {
    pub file_id: usize,
    pub tokens: Vec<tokens::Token>,
    pub const_macros: Vec<ConstMacro>,
    pub macro_funcs: Vec<MacroFunc>,
    pub modules: Vec<usize>,
    pub errors: Vec<LexError>,
}

pub(crate) struct LexContext {
    parts_list: Arc<RwLock<Vec<(PathBuf, Part)>>>,
    job_queue: Arc<Mutex<Vec<usize>>>,
}

pub(crate) struct Lexer {
    worker_id: usize,
    file_id: usize,
    outstanding_module_requests: usize,
    tokens: Vec<Token>,
    const_macros: Vec<ConstMacro>,
    macro_funcs: Vec<MacroFunc>,
    errors: Vec<LexError>,
    pos: Position,
    rd: Option<BufReader<File>>,
    file: Option<File>,
    rx: Receiver<SchedulerResponse<Lexer>>,
    tx: Sender<(usize, WorkerRequest<Lexer>)>,
}

#[derive(Clone, Copy)]
pub(crate) struct Position {
    file_id: usize,
    line: usize,
    column_bytes: usize,
    column_graphemes: usize,
}

pub(crate) struct ModIdRequest {
    parent_id: usize,
    name: String,
    pos: Position,
}

impl LexContext {
    pub(crate) fn new(
        parts_list: Arc<RwLock<Vec<(PathBuf, Part)>>>,
        job_queue: Arc<Mutex<Vec<usize>>>,
    ) -> Self {
        Self {
            parts_list,
            job_queue,
        }
    }
}

impl Stage for Lexer {
    type Job = File;
    type Result = LexResult;
    type Request = ModIdRequest;
    type Response = Result<usize, BadIncludeError>;
    type Context = LexContext;

    fn new(
        id: usize,
        rx: Receiver<SchedulerResponse<Self>>,
        tx: Sender<(usize, WorkerRequest<Self>)>,
    ) -> Self {
        Self {
            worker_id: id,
            file_id: 0,
            outstanding_module_requests: 0,
            tokens: Vec::new(),
            const_macros: Vec::new(),
            macro_funcs: Vec::new(),
            errors: Vec::new(),
            pos: Position::new(0),
            rd: None,
            file: None,
            rx,
            tx,
        }
    }

    fn spin_up(mut self) -> Result<(), SchedulerError> {
        let (mut file_id, mut file) = match self.rx.recv()? {
            SchedulerResponse::NewJob(file_id, file) => (file_id, file),
            SchedulerResponse::Respond(_) => {
                return Err(LexError::from(BadResponseError).into());
            }
            SchedulerResponse::ShutDown => return Ok(()),
        };

        loop {
            let mut result = self.lex_file(file_id, file)?;

            while self.outstanding_module_requests > 0 {
                match self.rx.recv().unwrap() {
                    SchedulerResponse::NewJob(_, _) => {
                        return Err(LexError::from(BadResponseError).into());
                    }
                    SchedulerResponse::Respond(mod_id) => {
                        match mod_id {
                            Ok(val) => result.modules.push(val),
                            Err(err) => result.errors.push(err.into()),
                        }
                        self.outstanding_module_requests -= 1;
                    }
                    SchedulerResponse::ShutDown => return Ok(()),
                }
            }

            if self
                .tx
                .send((self.worker_id, WorkerRequest::NewJob(result)))
                .is_err()
            {
                return Err(
                    LexError::from(FailedRequestError::new(self.worker_id, "New Job")).into(),
                );
            }

            (file_id, file) = match self.rx.recv()? {
                SchedulerResponse::NewJob(file_id, file) => (file_id, file),
                SchedulerResponse::Respond(_) => {
                    return Err(LexError::from(BadResponseError).into());
                }
                SchedulerResponse::ShutDown => return Ok(()),
            };
        }
    }

    fn handle_request(rq: Self::Request, ctx: &mut Self::Context) -> Self::Response {
        let mut split = rq.name.split("::").peekable();
        let mut path: PathBuf;
        let modules = ctx.parts_list.read().unwrap();
        if split.next_if_eq(&"base").is_some() {
            path = PathBuf::new();
        } else {
            path = modules[rq.parent_id].0.clone();
        }
        path = path.parent().unwrap_or(Path::new("")).to_path_buf();
        for str in split {
            path = path.join(str);
        }

        for (i, tu) in modules.iter().enumerate() {
            if tu.0 == path {
                return Ok(i);
            }
        }
        let old_len = modules.len();
        drop(modules);

        let id = File::open(path.with_added_extension("cfuq"));
        if let Ok(file) = id {
            let mut modules = ctx.parts_list.write().unwrap();
            for (i, tu) in modules[old_len..].iter().enumerate() {
                if tu.0 == path {
                    return Ok(old_len + i);
                }
            }
            let idx = modules.len();
            modules.push((path, Part::new(file, rq.name)));
            drop(modules);
            let mut jobs = ctx.job_queue.lock().unwrap();
            jobs.push(idx);
            Ok(idx)
        } else {
            let gcount = rq.name.graphemes(true).count();
            Err(BadIncludeError::new(rq.name, rq.pos.as_provenance(gcount)))
        }
    }

    fn new_job(ctx: &mut Self::Context) -> Result<Option<Self::Job>, SchedulerError> {
        let mut job_queue = ctx.job_queue.lock()?;
        let mut modules = ctx.parts_list.write()?;
        if job_queue.is_empty() {
            Ok(None)
        } else {
            let file = modules[job_queue[0]].1.start_processing()?;
            let file = match file {
                PartState::Source(source_file) => source_file,
                PartState::Processing => return Err(AlreadyProcessingError.into()),
                PartState::Tokens(_) => {
                    return Err(WrongStageError::new(PartStage::Tokens, PartStage::Source).into());
                }
            };
            job_queue.swap_remove(0);
            Ok(Some(file))
        }
    }

    fn commit_work(result: Self::Result, ctx: &mut Self::Context) -> Result<(), SchedulerError> {
        let mut modules = ctx.parts_list.write()?;
        modules[result.file_id]
            .1
            .finish_processing(PartState::Tokens(result))?;
        Ok(())
    }
}

impl From<&mut Lexer> for LexResult {
    fn from(value: &mut Lexer) -> Self {
        Self {
            file_id: value.file_id,
            tokens: mem::take(&mut value.tokens),
            const_macros: mem::take(&mut value.const_macros),
            macro_funcs: mem::take(&mut value.macro_funcs),
            modules: Vec::new(),
            errors: mem::take(&mut value.errors),
        }
    }
}

impl Lexer {
    fn lex_file(&mut self, file_id: usize, file: File) -> Result<LexResult, LexError> {
        self.outstanding_module_requests = 0;
        self.tokens = Vec::new();
        self.const_macros = Vec::new();
        self.macro_funcs = Vec::new();
        self.errors = Vec::new();
        self.pos = Position::new(file_id);
        self.rd = Some(BufReader::new(file));
        loop {
            let mut buff = String::new();
            let count = match self.rd.as_mut().unwrap().read_line(&mut buff) {
                Ok(val) => val,
                Err(err) => return Err(err.into()),
            };
            if count == 0 {
                break;
            }
            let mut tmp = &buff[..];
            let (bytes, graphemes) = Lexer::strip_start(&mut tmp);
            self.pos.column_bytes += bytes;
            self.pos.column_graphemes += graphemes;
            tmp = tmp.trim_end();
            if tmp.is_empty() || tmp.starts_with('#') {
                if tmp.starts_with("#!") {
                    match self.handle_directives(&mut tmp) {
                        Err(LexError::FailedRequest(err)) => {
                            return Err(err.into());
                        }
                        Err(err) => self.errors.push(err),
                        Ok(()) => (),
                    }
                }
                self.pos.column_bytes = 1;
                self.pos.column_graphemes = 1;
                self.pos.line += 1;
                continue;
            }
            loop {
                let (tkn, rest) = self.peel_token(tmp);
                match tkn {
                    Ok(tkn) => self.tokens.push(tkn),
                    Err(err) => self.errors.push(err),
                }
                tmp = match rest {
                    Some(tail) => tail,
                    None => break,
                };
            }
            self.tokens.push(tokens::Token {
                kind: tokens::TokenKind::Delim(DelimKind::EndL),
                prov: self.pos.as_provenance(1),
            });
            self.pos.column_bytes = 1;
            self.pos.column_graphemes = 1;
            self.pos.line += 1;
        }

        Ok(self.into())
    }

    fn handle_directives(&mut self, tmp: &mut &str) -> Result<(), LexError> {
        if let Some(postfix) = tmp.strip_prefix("#!def ") {
            *tmp = postfix;
            if tmp.contains('(') {
                if let Err(err) = self.handle_macro_funcs(tmp) {
                    self.errors.push(err.into());
                }
            } else {
                self.handle_const_macros(tmp);
            }
        } else if let Some(postfix) = tmp.strip_prefix("#!also ") {
            self.outstanding_module_requests += 1;
            *tmp = postfix;
            if self
                .tx
                .send((
                    self.worker_id,
                    WorkerRequest::Request(ModIdRequest {
                        parent_id: self.file_id,
                        name: tmp.to_string(),
                        pos: self.pos,
                    }),
                ))
                .is_err()
            {
                return Err(FailedRequestError::new(self.worker_id, "ModID Request").into());
            }
            return Ok(());
        } else {
            let result = self.tokenize(tmp).0;
            match result {
                Ok(val) => self.tokens.push(val),
                Err(err) => self.errors.push(err),
            }
        }
        Ok(())
    }

    fn handle_macro_funcs(&mut self, buff: &mut &str) -> Result<(), std::io::Error> {
        let mut sig_tokens = Vec::new();
        let mut def_tokens = Vec::new();
        loop {
            let (result, tail) = self.peel_token(buff);

            match result {
                Ok(val) => {
                    if val.kind == TokenKind::Operator(OperatorKind::Assign) {
                        break;
                    }
                    sig_tokens.push(val);
                }
                Err(err) => self.errors.push(err),
            }

            if let Some(tail) = tail {
                *buff = tail;
            } else {
                let gcount = buff.graphemes(true).count();
                self.errors.push(
                    NoMacroDefError {
                        bad: buff.to_string(),
                        prov: self.pos.as_provenance(gcount),
                    }
                    .into(),
                );
                return Ok(());
            }
        }
        if let Some(buff) = buff.trim_start().strip_prefix('{') {
            let mut depth = 1;
            let mut buff = buff.to_string();
            let mut tail = &buff[..];
            while depth > 0 {
                while tail.is_empty() {
                    buff.clear();
                    if self.rd.as_mut().unwrap().read_line(&mut buff)? == 0 {
                        self.errors.push(
                            UnclosedMacroError {
                                prov: self.pos.as_provenance(1),
                            }
                            .into(),
                        );
                        return Ok(());
                    }
                    tail = &buff[..];
                    self.pos.column_bytes = 1;
                    self.pos.column_graphemes = 1;
                    self.pos.line += 1;
                }
                let (result, tail_maybe) = self.peel_token(tail);
                match result {
                    Ok(val) => {
                        if val.kind == TokenKind::Delim(DelimKind::BlockO) {
                            depth += 1;
                        } else if val.kind == TokenKind::Delim(DelimKind::BlockC) {
                            depth -= 1;
                        }
                        def_tokens.push(val);
                    }
                    Err(err) => self.errors.push(err),
                }

                if let Some(tail_maybe) = tail_maybe {
                    tail = tail_maybe;
                } else {
                    tail = "";
                }
            }
            self.macro_funcs
                .push(MacroFunc::Tokens(sig_tokens, def_tokens));
            return Ok(());
        }
        let gcount = buff.graphemes(true).count();
        self.errors.push(
            NoMacroDefError {
                bad: buff.to_string(),
                prov: self.pos.as_provenance(gcount),
            }
            .into(),
        );
        Ok(())
    }

    fn handle_const_macros(&mut self, tmp: &str) {
        let idx = tmp.find(|c: char| c.is_whitespace());
        if let Some(val) = idx {
            let mut tmp = tmp.to_string();
            let tail = tmp.split_off(val);
            if tail.is_empty() {
                let gcount = tmp.graphemes(true).count();
                self.errors.push(
                    NoMacroDefError {
                        bad: tmp,
                        prov: self.pos.as_provenance(gcount),
                    }
                    .into(),
                );
            } else {
                let mut tokens = Vec::new();
                let mut tail = &tail[..];
                loop {
                    let (token, maybe_tail) = self.peel_token(tail);
                    match token {
                        Ok(token) => tokens.push(token),
                        Err(err) => self.errors.push(err),
                    }
                    let Some(new_tail) = maybe_tail else {
                        break;
                    };
                    tail = new_tail;
                }
                self.const_macros.push(ConstMacro::Tokens(tmp, tokens));
            }
        } else {
            let gcount = tmp.graphemes(true).count();
            self.errors.push(
                NoMacroDefError {
                    bad: tmp.to_string(),
                    prov: self.pos.as_provenance(gcount),
                }
                .into(),
            );
        }
    }

    fn peel_token<'a>(
        &mut self,
        string: &'a str,
    ) -> (Result<tokens::Token, LexError>, Option<&'a str>) {
        let mut string = string;
        let (bytes, graphemes) = Lexer::strip_start(&mut string);
        self.pos.column_bytes += bytes;
        self.pos.column_graphemes += graphemes;
        let (lit, consumed) = self.parse_literal(string);
        let lit = match lit {
            Ok(val) => val,
            Err(err) => {
                let tail = &string[consumed..];
                return (Err(err.into()), (!tail.is_empty()).then_some(tail));
            }
        };
        if let Some(lit) = lit {
            let tail = &string[consumed..];
            return (Ok(lit), (!tail.is_empty()).then_some(tail));
        }
        let bytes = string.as_bytes();
        for (i, c) in bytes.iter().enumerate() {
            match c {
                b' ' | b'\t' => {
                    let (token, _) = self.tokenize(&string[0..i]);
                    return (token, Some(&string[i..]));
                }
                _ if Lexer::is_opdelim(*c) => {
                    if i == 0 {
                        for (idx, (txt, tkn)) in DELIMS.iter().enumerate() {
                            if idx < MULTI_DELIMS {
                                if bytes.len() < 2 {
                                    continue;
                                }
                                if &bytes[..2] == txt.as_bytes() {
                                    let tail = &string[2..];
                                    let token = Token::new(Delim(*tkn), self.pos.as_provenance(2));
                                    self.pos.column_bytes += 2;
                                    self.pos.column_graphemes += 2;
                                    return (Ok(token), (!tail.is_empty()).then_some(tail));
                                }
                            } else if *c == txt.as_bytes()[0] {
                                let token = Token::new(Delim(*tkn), self.pos.as_provenance(1));
                                let tail = &string[1..];
                                self.pos.column_bytes += 1;
                                self.pos.column_graphemes += 1;
                                return (Ok(token), (!tail.is_empty()).then_some(tail));
                            }
                        }
                        for (idx, (txt, tkn)) in OPS.iter().enumerate() {
                            if idx < MULTI_OPS {
                                if bytes.len() < 2 {
                                    continue;
                                }
                                if &bytes[..2] == txt.as_bytes() {
                                    let tail = &string[2..];
                                    let token =
                                        Token::new(Operator(*tkn), self.pos.as_provenance(2));
                                    self.pos.column_bytes += 2;
                                    self.pos.column_graphemes += 2;
                                    return (Ok(token), (!tail.is_empty()).then_some(tail));
                                }
                            } else if &bytes[..1] == txt.as_bytes() {
                                let token = Token::new(Operator(*tkn), self.pos.as_provenance(1));
                                let tail = &string[1..];
                                self.pos.column_bytes += 1;
                                self.pos.column_graphemes += 1;
                                return (Ok(token), (!tail.is_empty()).then_some(tail));
                            }
                        }
                        let err = Err(BadOperatorError {
                            bad: String::from(&string[..1]),
                            prov: self.pos.as_provenance(1),
                        }
                        .into());
                        self.pos.column_bytes += 1;
                        self.pos.column_graphemes += 1;
                        let tail = &string[1..];
                        return (err, (!tail.is_empty()).then_some(tail));
                    }
                    let (token, size) = self.tokenize(&string[0..i]);
                    let tail = &string[size..];
                    return (token, (!tail.is_empty()).then_some(tail));
                }
                _ => (),
            }
        }
        let (token, size) = self.tokenize(string);
        self.pos.column_bytes += size;
        self.pos.column_graphemes += string.graphemes(true).count();
        (token, None)
    }

    fn tokenize(&mut self, string: &str) -> (Result<tokens::Token, LexError>, usize) {
        if let Some(string) = string.strip_prefix('$') {
            let gcount = string.graphemes(true).count();
            let result = (
                Ok(Token::new(
                    MacroInv(ModuleScopedId::Unresolved(string.to_string())),
                    self.pos.as_provenance(gcount),
                )),
                string.len(),
            );
            self.pos.column_bytes += string.len();
            self.pos.column_graphemes += gcount;
            return result;
        }
        if let Some(string) = string.strip_prefix("#!") {
            for (spelling, kind) in DIRECTIVES {
                if string.starts_with(*spelling) {
                    let gcount = string.graphemes(true).count();
                    let result = Ok(Token::new(Directive(*kind), self.pos.as_provenance(gcount)));
                    self.pos.column_bytes += string.len();
                    self.pos.column_graphemes += gcount;
                    return (result, string.len());
                }
            }
            let gcount = string
                .split_ascii_whitespace()
                .next()
                .unwrap_or_default()
                .graphemes(true)
                .count();
            let err = Err(BadDirectiveError {
                bad: string.to_string(),
                prov: self.pos.as_provenance(gcount),
            }
            .into());
            self.pos.column_bytes += string.len();
            self.pos.column_graphemes += gcount;
            return (err, string.len());
        }
        for (spelling, kind) in KEYWORDS {
            if *spelling == string {
                let result = (
                    Ok(Token::new(
                        Keyword(*kind),
                        self.pos.as_provenance(string.len()),
                    )),
                    string.len(),
                );
                self.pos.column_bytes += string.len();
                self.pos.column_graphemes += string.len();
                return result;
            }
        }

        if string.starts_with(|c: char| c.is_alphabetic())
            && string.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            let gcount = string.graphemes(true).count();
            let result = Ok(Token::new(
                tokens::TokenKind::Identifier(ModuleScopedId::Unresolved(string.to_string())),
                self.pos.as_provenance(gcount),
            ));
            self.pos.column_bytes += string.len();
            self.pos.column_graphemes += gcount;
            return (result, string.len());
        }
        let gcount = string.graphemes(true).count();
        let err = BadIdentifierError {
            bad: string.to_string(),
            prov: self.pos.as_provenance(gcount),
        };
        self.pos.column_bytes += string.len();
        self.pos.column_graphemes += gcount;
        (Err(err.into()), string.len())
    }

    /// #Returns
    /// The stripped size in (bytes, graphemes)
    fn strip_start(line: &mut &str) -> (usize, usize) {
        let tmp = line.trim_start();
        let diff = (
            line.len() - tmp.len(),
            line.graphemes(true).count() - tmp.graphemes(true).count(),
        );
        *line = tmp;
        diff
    }

    fn is_opdelim(char: u8) -> bool {
        matches!(
            char,
            b'=' | b'-'
                | b'+'
                | b'*'
                | b'/'
                | b'%'
                | b'<'
                | b'>'
                | b'&'
                | b'|'
                | b'^'
                | b','
                | b'!'
                | b'~'
                | b'{'
                | b'}'
                | b'('
                | b')'
                | b'['
                | b']'
                | b':'
                | b'#'
        )
    }

    /// # Returns
    /// The first tuple element represents literal parse state: Successfully parsed `Ok(Some)`,
    /// None found `Ok(None)`, and a parse error `Err(BadLiteralError)`
    ///
    /// The second represents the amount of bytes to consume, 0 if no literal was found
    fn parse_literal(&mut self, string: &str) -> (Result<Option<Token>, BadLiteralError>, usize) {
        let (res, num) = self.parse_byte(string);
        match res {
            Ok(None) => (),
            value => return (value, num),
        }

        let (res, num) = self.parse_char(string);
        match res {
            Ok(None) => (),
            value => return (value, num),
        }

        let (res, num) = self.parse_string(string);
        match res {
            Ok(None) => (),
            value => return (value, num),
        }
        self.parse_number(string)
    }

    fn parse_byte(&mut self, str: &str) -> (Result<Option<Token>, BadLiteralError>, usize) {
        if let Some(str) = str.strip_prefix("h'") {
            for (i, c) in str.as_bytes().iter().enumerate() {
                if c.is_ascii_whitespace() || Lexer::is_opdelim(*c) {
                    let slice = &str[..i];
                    if slice.chars().any(|c: char| !c.is_ascii_hexdigit()) {
                        let gcount = slice.graphemes(true).count();
                        let err = Err(BadLiteralError {
                            bad: slice.to_string(),
                            prov: self.pos.as_provenance(gcount),
                        });
                        self.pos.column_bytes += i + 2;
                        self.pos.column_graphemes += gcount + 2;
                        return (err, i + 2);
                    }
                    let result = Ok(Some(Token::new(
                        Literal(LiteralKind::Byte(slice.to_ascii_lowercase())),
                        self.pos.as_provenance(i + 2),
                    )));

                    self.pos.column_bytes += i + 2;
                    self.pos.column_graphemes += i + 2;
                    return (result, i + 2);
                }
            }
        } else if let Some(postfix) = str.strip_prefix("b'") {
            if str.len() < 3 {
                let err = Err(BadLiteralError {
                    bad: String::from(str),
                    prov: self.pos.as_provenance(str.len()),
                });
                self.pos.column_bytes += str.len();
                self.pos.column_graphemes += str.len();
                return (err, str.len());
            }
            if str.as_bytes()[..3].is_ascii() {
                let ch = format!("{:02x}", str.as_bytes()[2]);
                let result = (
                    Ok(Some(Token::new(
                        Literal(LiteralKind::Byte(ch)),
                        self.pos.as_provenance(3),
                    ))),
                    3,
                );
                self.pos.column_bytes += 3;
                self.pos.column_graphemes += 3;
                return result;
            }
            let ch = postfix.graphemes(true).next().unwrap_or_default();
            let err = Err(BadLiteralError {
                bad: String::from(&str[..2]) + ch,
                prov: self.pos.as_provenance(3),
            });
            self.pos.column_bytes += 2 + ch.len();
            self.pos.column_graphemes += 3;
            return (err, 2 + ch.len());
        }
        (Ok(None), 0)
    }

    fn parse_char(&mut self, string: &str) -> (Result<Option<Token>, BadLiteralError>, usize) {
        if let Some(postfix) = string.strip_prefix('\'') {
            if string.len() < 2 {
                let err = Err(BadLiteralError {
                    bad: String::from(string),
                    prov: self.pos.as_provenance(string.len()),
                });
                self.pos.column_bytes += string.len();
                self.pos.column_graphemes += string.len();
                return (err, string.len());
            }
            if postfix[..1].is_ascii() {
                let ch: u8 = string.as_bytes()[1];
                let result = Ok(Some(Token::new(
                    Literal(LiteralKind::Char(ch)),
                    self.pos.as_provenance(2),
                )));
                self.pos.column_bytes += 2;
                self.pos.column_graphemes += 2;
                return (result, 2);
            }
            let ch = string[1..].graphemes(true).next().unwrap_or_default();
            let err = Err(BadLiteralError {
                bad: String::from("'") + ch,
                prov: self.pos.as_provenance(2),
            });
            self.pos.column_bytes += 1 + ch.len();
            self.pos.column_graphemes += 2;
            return (err, 1 + ch.len());
        }
        (Ok(None), 0)
    }

    fn parse_string(&mut self, string: &str) -> (Result<Option<Token>, BadLiteralError>, usize) {
        if let Some(postfix) = string.strip_prefix('"') {
            if string.len() < 2 {
                let err = Err(BadLiteralError {
                    bad: String::from(string),
                    prov: self.pos.as_provenance(string.len()),
                });
                self.pos.column_bytes += string.len();
                self.pos.column_graphemes += string.len();
                return (err, string.len());
            }
            let mut escaped = false;
            let string = postfix;
            for (i, c) in string.as_bytes().iter().enumerate() {
                if *c == b'\\' {
                    escaped = !escaped;
                } else if *c == b'"' && !escaped {
                    let gcount = 2 + string[..i].graphemes(true).count();
                    let result = Ok(Some(Token::new(
                        Literal(LiteralKind::String(String::from(&string[..i]))),
                        self.pos.as_provenance(gcount),
                    )));
                    self.pos.column_bytes += 2 + i;
                    self.pos.column_graphemes += gcount;
                    return (result, 2 + i);
                } else {
                    escaped = false;
                }
            }
            let err = Err(BadLiteralError {
                bad: String::from('"') + string,
                prov: self.pos.as_provenance(1 + string.len()),
            });
            self.pos.column_bytes += 1 + string.len();
            self.pos.column_graphemes += 1 + string.graphemes(true).count();
            return (err, 1 + string.len());
        }
        (Ok(None), 0)
    }

    fn parse_number(&mut self, str: &str) -> (Result<Option<Token>, BadLiteralError>, usize) {
        if str.starts_with(|c: char| c.is_ascii_digit()) {
            let mut is_float = false;
            for (i, c) in str.as_bytes().iter().enumerate() {
                if !c.is_ascii_digit() {
                    if *c == b'.' {
                        is_float = true;
                        continue;
                    }
                    if !c.is_ascii_whitespace() && !Lexer::is_opdelim(*c) {
                        continue;
                    }
                    if is_float {
                        let num = str[..i].parse::<f64>().map_err(|_| BadLiteralError {
                            bad: String::from(&str[..i]),
                            prov: self.pos.as_provenance(i),
                        });
                        let result = match num {
                            Ok(val) => val,
                            Err(e) => {
                                self.pos.column_bytes += i;
                                self.pos.column_graphemes += str[..i].graphemes(true).count();
                                return (Err(e), i);
                            }
                        };

                        let result = Ok(Some(Token::new(
                            Literal(LiteralKind::Float(result)),
                            self.pos.as_provenance(i),
                        )));
                        self.pos.column_bytes += i;
                        self.pos.column_graphemes += str[..i].graphemes(true).count();
                        return (result, i);
                    }
                    let slice = &str[..i];
                    if slice.chars().any(|c: char| !c.is_ascii_digit()) {
                        let gcount = slice.graphemes(true).count();
                        let err = Err(BadLiteralError {
                            bad: slice.to_string(),
                            prov: self.pos.as_provenance(gcount),
                        });
                        self.pos.column_bytes += i;
                        self.pos.column_graphemes += gcount;
                        return (err, i);
                    }
                    let result = Ok(Some(Token::new(
                        Literal(LiteralKind::Int(slice.to_string())),
                        self.pos.as_provenance(i),
                    )));

                    self.pos.column_bytes += i;
                    self.pos.column_graphemes += i;
                    return (result, i);
                }
            }
            if is_float {
                let num = str.parse::<f64>().map_err(|_| BadLiteralError {
                    bad: String::from(str),
                    prov: self.pos.as_provenance(str.graphemes(true).count()),
                });
                let num = match num {
                    Ok(val) => Ok(Some(Token::new(
                        Literal(LiteralKind::Float(val)),
                        self.pos.as_provenance(str.graphemes(true).count()),
                    ))),
                    Err(e) => Err(e),
                };
                self.pos.column_bytes += str.len();
                self.pos.column_graphemes += str.graphemes(true).count();
                return (num, str.len());
            }
            if str.chars().any(|c: char| !c.is_ascii_digit()) {
                let gcount = str.graphemes(true).count();
                let err = Err(BadLiteralError {
                    bad: str.to_string(),
                    prov: self.pos.as_provenance(gcount),
                });
                self.pos.column_bytes += str.len();
                self.pos.column_graphemes += gcount;
                return (err, str.len());
            }
            let result = Ok(Some(Token::new(
                Literal(LiteralKind::Int(str.to_string())),
                self.pos.as_provenance(str.len()),
            )));
            self.pos.column_bytes += str.len();
            self.pos.column_graphemes += str.len();
            return (result, str.len());
        }
        (Ok(None), 0)
    }
}

impl Position {
    fn new(file_id: usize) -> Self {
        Self {
            file_id,
            line: 1,
            column_bytes: 1,
            column_graphemes: 1,
        }
    }

    pub(crate) fn as_provenance(&self, size: usize) -> Provenance {
        Provenance::new(self.file_id, self.line, self.column_graphemes, size)
    }
}

#[derive(Debug)]
pub enum LexError {
    Io(std::io::Error),
    BadInclude(BadIncludeError),
    BadToken(BadTokenError),
    BadDirective(BadDirectiveError),
    BadLiteral(BadLiteralError),
    BadOperator(BadOperatorError),
    BadIdentifier(BadIdentifierError),
    NoMacroDef(NoMacroDefError),
    UnclosedMacro(UnclosedMacroError),
    BadResponse(BadResponseError),
    FailedRequest(FailedRequestError),
}

#[derive(Debug)]
pub struct BadIncludeError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct NoMacroDefError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct BadDirectiveError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct BadTokenError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct BadLiteralError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct BadOperatorError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct BadIdentifierError {
    bad: String,
    prov: Provenance,
}

#[derive(Debug)]
pub struct UnclosedMacroError {
    prov: Provenance,
}

#[derive(Debug)]
pub struct BadResponseError;

impl LexError {
    pub fn provenance(&self) -> Option<&Provenance> {
        match self {
            Self::Io(_) | Self::BadResponse(_) | Self::FailedRequest(_) => None,
            Self::BadInclude(val) => Some(&val.prov),
            Self::NoMacroDef(val) => Some(&val.prov),
            Self::BadToken(val) => Some(&val.prov),
            Self::BadDirective(val) => Some(&val.prov),
            Self::BadOperator(val) => Some(&val.prov),
            Self::BadLiteral(val) => Some(&val.prov),
            Self::BadIdentifier(val) => Some(&val.prov),
            Self::UnclosedMacro(val) => Some(&val.prov),
        }
    }
}

impl BadIncludeError {
    pub(crate) fn new(bad: String, prov: Provenance) -> Self {
        Self { bad, prov }
    }
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => err.fmt(f),
            Self::BadInclude(err) => err.fmt(f),
            Self::NoMacroDef(err) => err.fmt(f),
            Self::BadToken(err) => err.fmt(f),
            Self::BadDirective(err) => err.fmt(f),
            Self::BadLiteral(err) => err.fmt(f),
            Self::BadOperator(err) => err.fmt(f),
            Self::BadIdentifier(err) => err.fmt(f),
            Self::UnclosedMacro(err) => err.fmt(f),
            Self::BadResponse(err) => err.fmt(f),
            Self::FailedRequest(err) => err.fmt(f),
        }
    }
}

impl fmt::Display for BadIncludeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = &self.bad[..];
        write!(f, "Could not find \"{result}\" module!")
    }
}

impl fmt::Display for NoMacroDefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = &self.bad[..];
        write!(f, "Macro \"{result}\" is missing a definition!")
    }
}

impl fmt::Display for BadTokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = &self.bad[..];
        write!(f, "\"{result}\" could not be parsed as a token!")
    }
}

impl fmt::Display for BadDirectiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = &self.bad[..];
        write!(f, "\"{result}\" is not a recognized directive!")
    }
}

impl fmt::Display for BadLiteralError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = &self.bad[..];
        write!(f, "\"{result}\" is a malformed literal!")
    }
}

impl fmt::Display for BadOperatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let result = &self.bad[..];
        write!(f, "\"{result}\" is an unrecognized operator!")
    }
}

impl fmt::Display for BadIdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bad = &self.bad[..];
        write!(f, "\"{bad}\" is an invalid identifier!")
    }
}

impl fmt::Display for UnclosedMacroError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Could not find closing brace of macro function block!")
    }
}

impl fmt::Display for BadResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Scheduler sent unexpected response type!")
    }
}

impl From<std::io::Error> for LexError {
    fn from(err: std::io::Error) -> Self {
        LexError::Io(err)
    }
}

impl From<BadIncludeError> for LexError {
    fn from(err: BadIncludeError) -> Self {
        LexError::BadInclude(err)
    }
}

impl From<NoMacroDefError> for LexError {
    fn from(err: NoMacroDefError) -> Self {
        LexError::NoMacroDef(err)
    }
}

impl From<BadDirectiveError> for LexError {
    fn from(err: BadDirectiveError) -> Self {
        LexError::BadDirective(err)
    }
}

impl From<BadTokenError> for LexError {
    fn from(err: BadTokenError) -> Self {
        LexError::BadToken(err)
    }
}

impl From<BadLiteralError> for LexError {
    fn from(err: BadLiteralError) -> Self {
        LexError::BadLiteral(err)
    }
}

impl From<BadOperatorError> for LexError {
    fn from(err: BadOperatorError) -> Self {
        LexError::BadOperator(err)
    }
}

impl From<BadIdentifierError> for LexError {
    fn from(err: BadIdentifierError) -> Self {
        LexError::BadIdentifier(err)
    }
}

impl From<UnclosedMacroError> for LexError {
    fn from(err: UnclosedMacroError) -> Self {
        LexError::UnclosedMacro(err)
    }
}

impl From<BadResponseError> for LexError {
    fn from(err: BadResponseError) -> Self {
        LexError::BadResponse(err)
    }
}

impl From<FailedRequestError> for LexError {
    fn from(err: FailedRequestError) -> Self {
        LexError::FailedRequest(err)
    }
}

impl Error for LexError {}
impl Error for BadIncludeError {}
impl Error for BadTokenError {}
impl Error for BadDirectiveError {}
impl Error for BadLiteralError {}
impl Error for BadOperatorError {}
impl Error for BadIdentifierError {}
impl Error for NoMacroDefError {}
impl Error for BadResponseError {}
