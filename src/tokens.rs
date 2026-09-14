use crate::common;

#[derive(Debug)]
pub struct Token {
    pub kind: TokenKind,
    pub prov: common::Provenance,
}

impl Token {
    pub fn new(kind: TokenKind, prov: common::Provenance) -> Self {
        Self { kind, prov }
    }
}

#[derive(Debug, PartialEq)]
pub enum TokenKind {
    Operator(OperatorKind),
    Delim(DelimKind),
    Identifier(common::ModuleScopedId),
    Directive(DirectiveKind),
    MacroInv(common::ModuleScopedId),
    Keyword(KeywordKind),
    Literal(LiteralKind),
}

#[derive(Debug, PartialEq)]
pub enum LiteralKind {
    Int(String),
    Float(f64),
    String(String),
    Char(u8),
    Byte(String),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DirectiveKind {
    If,
    Error,
    Log,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum KeywordKind {
    Function,
    TypeCheck,
    Thing,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OperatorKind {
    Assign,
    Add,
    Negate,
    Mult,
    Div,
    Mod,
    DivUp,
    Project,
    LogNot,
    LogAnd,
    LogOr,
    BitNot,
    BitAnd,
    BitOr,
    BitXor,
    Equal,
    NEqual,
    Less,
    More,
    LessEqual,
    MoreEqual,
    Return,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DelimKind {
    ComposeO,
    ComposeC,
    InvokeO,
    InvokeC,
    BlockO,
    BlockC,
    Sep,
    EndL,
    TypeMarker,
    SubPart,
}

#[allow(clippy::enum_glob_use)]
use OperatorKind::*;
pub const MULTI_OPS: usize = 9;
/// Single-byte operators are listed first, their count is stored in `SINGLE_OPS`
pub const OPS: &[(&str, OperatorKind)] = &[
    ("->", Project),
    ("=>", Return),
    ("//", DivUp),
    ("&&", LogAnd),
    ("||", LogOr),
    ("==", Equal),
    ("!=", NEqual),
    (">=", MoreEqual),
    ("<=", LessEqual),
    ("=", Assign),
    ("+", Add),
    ("-", Negate),
    ("*", Mult),
    ("/", Div),
    ("%", Mod),
    ("|", BitOr),
    ("~", BitNot),
    ("&", BitAnd),
    ("^", BitXor),
    ("!", LogNot),
    ("<", Less),
    (">", More),
];

#[allow(clippy::enum_glob_use)]
use DelimKind::*;
pub const MULTI_DELIMS: usize = 1;
/// Multi-byte delimiters are listed first, their count specified in `MULTI_DELIMS`
pub const DELIMS: &[(&str, DelimKind)] = &[
    ("::", SubPart),
    ("{", BlockO),
    ("}", BlockC),
    ("(", InvokeO),
    (")", InvokeC),
    ("[", ComposeO),
    ("]", ComposeC),
    (",", Sep),
    (":", TypeMarker),
];

#[allow(clippy::enum_glob_use)]
use KeywordKind::*;
pub const KEYWORDS: &[(&str, KeywordKind)] =
    &[("is", TypeCheck), ("fu", Function), ("thing", Thing)];

#[allow(clippy::enum_glob_use)]
use DirectiveKind::*;
pub const DIRECTIVES: &[(&str, DirectiveKind)] = &[("if ", If), ("err ", Error), ("log ", Log)];
