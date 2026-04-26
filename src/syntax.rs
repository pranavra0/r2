use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::{CaseBranch, Lambda, Pattern, RecBinding, Symbol, Term, Value, thunk};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyntaxError {
    message: String,
    line: usize,
    column: usize,
}

impl SyntaxError {
    fn new(source: &str, offset: usize, message: impl Into<String>) -> Self {
        let (line, column) = line_and_column(source, offset);
        Self {
            message: message.into(),
            line,
            column,
        }
    }

    pub fn line(&self) -> usize {
        self.line
    }

    pub fn column(&self) -> usize {
        self.column
    }
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}:{}", self.message, self.line, self.column)
    }
}

impl std::error::Error for SyntaxError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    start: usize,
    end: usize,
}

impl Span {
    fn join(self, other: Span) -> Self {
        Self {
            start: self.start,
            end: other.end,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TokenKind {
    Ident(String),
    Int(i64),
    String(Vec<u8>),
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Semicolon,
    Dot,
    Equal,
    EqualEqual,
    BangEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Arrow,
    Let,
    Rec,
    Fn,
    Perform,
    Handle,
    With,
    Match,
    If,
    Then,
    Else,
    True,
    False,
    Lazy,
    Force,
    Underscore,
    Eof,
}

impl TokenKind {
    fn describe(&self) -> String {
        match self {
            Self::Ident(_) => "identifier".to_string(),
            Self::Int(_) => "integer".to_string(),
            Self::String(_) => "string literal".to_string(),
            Self::LParen => "`(`".to_string(),
            Self::RParen => "`)`".to_string(),
            Self::LBrace => "`{`".to_string(),
            Self::RBrace => "`}`".to_string(),
            Self::LBracket => "`[`".to_string(),
            Self::RBracket => "`]`".to_string(),
            Self::Comma => "`,`".to_string(),
            Self::Colon => "`:`".to_string(),
            Self::Semicolon => "`;`".to_string(),
            Self::Dot => "`.`".to_string(),
            Self::Equal => "`=`".to_string(),
            Self::EqualEqual => "`==`".to_string(),
            Self::BangEqual => "`!=`".to_string(),
            Self::Less => "`<`".to_string(),
            Self::LessEqual => "`<=`".to_string(),
            Self::Greater => "`>`".to_string(),
            Self::GreaterEqual => "`>=`".to_string(),
            Self::Plus => "`+`".to_string(),
            Self::Minus => "`-`".to_string(),
            Self::Star => "`*`".to_string(),
            Self::Slash => "`/`".to_string(),
            Self::Percent => "`%`".to_string(),
            Self::Arrow => "`=>`".to_string(),
            Self::Let => "`let`".to_string(),
            Self::Rec => "`rec`".to_string(),
            Self::Fn => "`fn`".to_string(),
            Self::Perform => "`perform`".to_string(),
            Self::Handle => "`handle`".to_string(),
            Self::With => "`with`".to_string(),
            Self::Match => "`match`".to_string(),
            Self::If => "`if`".to_string(),
            Self::Then => "`then`".to_string(),
            Self::Else => "`else`".to_string(),
            Self::True => "`true`".to_string(),
            Self::False => "`false`".to_string(),
            Self::Lazy => "`lazy`".to_string(),
            Self::Force => "`force`".to_string(),
            Self::Underscore => "`_`".to_string(),
            Self::Eof => "end of input".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Expr {
    kind: ExprKind,
    span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ExprKind {
    Name(String),
    Integer(i64),
    Bytes(Vec<u8>),
    Bool(bool),
    Symbol(Symbol),
    List(Vec<Expr>),
    Record(Vec<(String, Expr)>),
    Let {
        name: String,
        value: Box<Expr>,
        body: Box<Expr>,
    },
    LetRec {
        bindings: Vec<RecExprBinding>,
        body: Box<Expr>,
    },
    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },
    Call {
        callee: Box<Expr>,
        args: Vec<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Perform {
        op: Symbol,
        args: Vec<Expr>,
    },
    Handle {
        body: Box<Expr>,
        handlers: Vec<Handler>,
    },
    Match {
        scrutinee: Box<Expr>,
        branches: Vec<MatchBranch>,
    },
    If {
        condition: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },
    Lazy(Box<Expr>),
    Force(Box<Expr>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    NotEq,
    Less,
    LessEq,
    Greater,
    GreaterEq,
}

impl BinaryOp {
    fn effect_op(self) -> Symbol {
        match self {
            Self::Add => Symbol::from("math.add"),
            Self::Sub => Symbol::from("math.sub"),
            Self::Mul => Symbol::from("math.mul"),
            Self::Div => Symbol::from("math.div"),
            Self::Rem => Symbol::from("math.rem"),
            Self::Eq => Symbol::from("math.eq"),
            Self::NotEq => Symbol::from("math.ne"),
            Self::Less => Symbol::from("math.lt"),
            Self::LessEq => Symbol::from("math.le"),
            Self::Greater => Symbol::from("math.gt"),
            Self::GreaterEq => Symbol::from("math.ge"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecExprBinding {
    name: String,
    params: Vec<String>,
    body: Expr,
    span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MatchBranch {
    pattern: ParsedPattern,
    body: Expr,
    span: Span,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ParsedPattern {
    Wildcard,
    Bind(String),
    Symbol(Symbol),
    Tagged {
        tag: Symbol,
        fields: Vec<ParsedPattern>,
    },
}

impl ParsedPattern {
    fn binding_names(&self, out: &mut Vec<String>) {
        match self {
            Self::Wildcard | Self::Symbol(_) => {}
            Self::Bind(name) => out.push(name.clone()),
            Self::Tagged { fields, .. } => {
                for field in fields {
                    field.binding_names(out);
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Handler {
    op: Symbol,
    params: Vec<String>,
    body: Expr,
    span: Span,
}

pub fn parse(source: &str) -> Result<Term, SyntaxError> {
    let tokens = Lexer::new(source).tokenize()?;
    let expr = Parser::new(source, tokens).parse_program()?;
    lower_expr(source, &expr, &mut Vec::new())
}

struct Lexer<'a> {
    source: &'a str,
    offset: usize,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self { source, offset: 0 }
    }

    fn tokenize(mut self) -> Result<Vec<Token>, SyntaxError> {
        let mut tokens = Vec::new();

        loop {
            self.skip_trivia();

            if self.offset >= self.source.len() {
                tokens.push(Token {
                    kind: TokenKind::Eof,
                    span: Span {
                        start: self.offset,
                        end: self.offset,
                    },
                });
                return Ok(tokens);
            }

            let token = self.next_token()?;
            tokens.push(token);
        }
    }

    fn skip_trivia(&mut self) {
        loop {
            let Some(ch) = self.peek_char() else {
                return;
            };

            if ch.is_whitespace() {
                self.offset += ch.len_utf8();
                continue;
            }

            if ch == '#' {
                while let Some(next) = self.peek_char() {
                    self.offset += next.len_utf8();
                    if next == '\n' {
                        break;
                    }
                }
                continue;
            }

            break;
        }
    }

    fn next_token(&mut self) -> Result<Token, SyntaxError> {
        let start = self.offset;
        let ch = self
            .peek_char()
            .expect("next_token should only run before eof");

        let kind = match ch {
            '(' => {
                self.offset += 1;
                TokenKind::LParen
            }
            ')' => {
                self.offset += 1;
                TokenKind::RParen
            }
            '{' => {
                self.offset += 1;
                TokenKind::LBrace
            }
            '}' => {
                self.offset += 1;
                TokenKind::RBrace
            }
            '[' => {
                self.offset += 1;
                TokenKind::LBracket
            }
            ']' => {
                self.offset += 1;
                TokenKind::RBracket
            }
            ',' => {
                self.offset += 1;
                TokenKind::Comma
            }
            ':' => {
                self.offset += 1;
                TokenKind::Colon
            }
            ';' => {
                self.offset += 1;
                TokenKind::Semicolon
            }
            '.' => {
                self.offset += 1;
                TokenKind::Dot
            }
            '=' => {
                if self.source[self.offset..].starts_with("=>") {
                    self.offset += 2;
                    TokenKind::Arrow
                } else if self.source[self.offset..].starts_with("==") {
                    self.offset += 2;
                    TokenKind::EqualEqual
                } else {
                    self.offset += 1;
                    TokenKind::Equal
                }
            }
            '!' => {
                if self.source[self.offset..].starts_with("!=") {
                    self.offset += 2;
                    TokenKind::BangEqual
                } else {
                    return Err(SyntaxError::new(
                        self.source,
                        start,
                        "expected `!=`, found `!`",
                    ));
                }
            }
            '<' => {
                if self.source[self.offset..].starts_with("<=") {
                    self.offset += 2;
                    TokenKind::LessEqual
                } else {
                    self.offset += 1;
                    TokenKind::Less
                }
            }
            '>' => {
                if self.source[self.offset..].starts_with(">=") {
                    self.offset += 2;
                    TokenKind::GreaterEqual
                } else {
                    self.offset += 1;
                    TokenKind::Greater
                }
            }
            '+' => {
                self.offset += 1;
                TokenKind::Plus
            }
            '-' => {
                self.offset += 1;
                TokenKind::Minus
            }
            '*' => {
                self.offset += 1;
                TokenKind::Star
            }
            '/' => {
                self.offset += 1;
                TokenKind::Slash
            }
            '%' => {
                self.offset += 1;
                TokenKind::Percent
            }
            '"' => self.lex_string()?,
            '0'..='9' => self.lex_integer()?,
            _ if is_ident_start(ch) => self.lex_identifier(),
            _ => {
                return Err(SyntaxError::new(
                    self.source,
                    start,
                    format!("unexpected character `{ch}`"),
                ));
            }
        };

        Ok(Token {
            kind,
            span: Span {
                start,
                end: self.offset,
            },
        })
    }

    fn lex_string(&mut self) -> Result<TokenKind, SyntaxError> {
        self.offset += 1;
        let mut bytes = Vec::new();

        while let Some(ch) = self.peek_char() {
            match ch {
                '"' => {
                    self.offset += 1;
                    return Ok(TokenKind::String(bytes));
                }
                '\\' => {
                    self.offset += 1;
                    let Some(escaped) = self.peek_char() else {
                        return Err(SyntaxError::new(
                            self.source,
                            self.offset,
                            "unterminated escape sequence",
                        ));
                    };
                    self.offset += escaped.len_utf8();
                    match escaped {
                        '"' => bytes.push(b'"'),
                        '\\' => bytes.push(b'\\'),
                        'n' => bytes.push(b'\n'),
                        'r' => bytes.push(b'\r'),
                        't' => bytes.push(b'\t'),
                        '0' => bytes.push(0),
                        _ => {
                            return Err(SyntaxError::new(
                                self.source,
                                self.offset - escaped.len_utf8(),
                                format!("unsupported escape `\\{escaped}`"),
                            ));
                        }
                    }
                }
                _ => {
                    let end = self.offset + ch.len_utf8();
                    bytes.extend_from_slice(&self.source.as_bytes()[self.offset..end]);
                    self.offset = end;
                }
            }
        }

        Err(SyntaxError::new(
            self.source,
            self.source.len(),
            "unterminated string literal",
        ))
    }

    fn lex_integer(&mut self) -> Result<TokenKind, SyntaxError> {
        let start = self.offset;
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                self.offset += ch.len_utf8();
            } else {
                break;
            }
        }

        let value = self.source[start..self.offset]
            .parse::<i64>()
            .map_err(|_| SyntaxError::new(self.source, start, "integer literal is out of range"))?;
        Ok(TokenKind::Int(value))
    }

    fn lex_identifier(&mut self) -> TokenKind {
        let start = self.offset;
        while let Some(ch) = self.peek_char() {
            if is_ident_continue(ch) {
                self.offset += ch.len_utf8();
            } else {
                break;
            }
        }

        match &self.source[start..self.offset] {
            "let" => TokenKind::Let,
            "rec" => TokenKind::Rec,
            "fn" => TokenKind::Fn,
            "perform" => TokenKind::Perform,
            "handle" => TokenKind::Handle,
            "with" => TokenKind::With,
            "match" => TokenKind::Match,
            "if" => TokenKind::If,
            "then" => TokenKind::Then,
            "else" => TokenKind::Else,
            "true" => TokenKind::True,
            "false" => TokenKind::False,
            "lazy" => TokenKind::Lazy,
            "force" => TokenKind::Force,
            "_" => TokenKind::Underscore,
            name => TokenKind::Ident(name.to_string()),
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.source[self.offset..].chars().next()
    }
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl<'a> Parser<'a> {
    fn new(source: &'a str, tokens: Vec<Token>) -> Self {
        Self {
            source,
            tokens,
            cursor: 0,
        }
    }

    fn parse_program(&mut self) -> Result<Expr, SyntaxError> {
        let expr = self.parse_expr()?;
        if !matches!(self.peek().kind, TokenKind::Eof) {
            return Err(self.error_here(format!(
                "expected end of input, found {}",
                self.peek().kind.describe()
            )));
        }
        Ok(expr)
    }

    fn parse_expr(&mut self) -> Result<Expr, SyntaxError> {
        self.parse_let()
    }

    fn parse_let(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Let) {
            return self.parse_handle();
        }

        let start = self.next().span;
        if matches!(self.peek().kind, TokenKind::Rec) {
            return self.parse_let_rec(start);
        }
        let (name, _) = self.expect_ident()?;
        self.expect_token("`=`", |kind| matches!(kind, TokenKind::Equal))?;
        let value = self.parse_expr()?;
        self.expect_token("`;`", |kind| matches!(kind, TokenKind::Semicolon))?;
        let body = self.parse_expr()?;
        let span = start.join(body.span);

        Ok(Expr {
            kind: ExprKind::Let {
                name,
                value: Box::new(value),
                body: Box::new(body),
            },
            span,
        })
    }

    fn parse_let_rec(&mut self, start: Span) -> Result<Expr, SyntaxError> {
        self.expect_token("`rec`", |kind| matches!(kind, TokenKind::Rec))?;
        let mut bindings = Vec::new();

        loop {
            bindings.push(self.parse_rec_binding()?);
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.next();
            } else {
                break;
            }
        }

        self.expect_token("`;`", |kind| matches!(kind, TokenKind::Semicolon))?;
        let body = self.parse_expr()?;
        let span = start.join(body.span);

        Ok(Expr {
            kind: ExprKind::LetRec {
                bindings,
                body: Box::new(body),
            },
            span,
        })
    }

    fn parse_rec_binding(&mut self) -> Result<RecExprBinding, SyntaxError> {
        let start = self.peek().span;
        let (name, _) = self.expect_ident()?;
        self.expect_token("`=`", |kind| matches!(kind, TokenKind::Equal))?;
        self.expect_token("`fn`", |kind| matches!(kind, TokenKind::Fn))?;
        self.expect_token("`(`", |kind| matches!(kind, TokenKind::LParen))?;
        let params = self.parse_ident_list(|kind| matches!(kind, TokenKind::RParen))?;
        self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;
        self.expect_token("`=>`", |kind| matches!(kind, TokenKind::Arrow))?;
        let body = self.parse_expr()?;
        let span = start.join(body.span);

        Ok(RecExprBinding {
            name,
            params,
            body,
            span,
        })
    }

    fn parse_handle(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Handle) {
            return self.parse_if();
        }

        let start = self.next().span;
        let body = self.parse_expr()?;
        self.expect_token("`with`", |kind| matches!(kind, TokenKind::With))?;
        self.expect_token("`{`", |kind| matches!(kind, TokenKind::LBrace))?;
        let mut handlers = Vec::new();

        if !matches!(self.peek().kind, TokenKind::RBrace) {
            loop {
                handlers.push(self.parse_handler()?);
                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.next();
                    if matches!(self.peek().kind, TokenKind::RBrace) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        let end = self.expect_token("`}`", |kind| matches!(kind, TokenKind::RBrace))?;
        Ok(Expr {
            kind: ExprKind::Handle {
                body: Box::new(body),
                handlers,
            },
            span: start.join(end),
        })
    }

    fn parse_if(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::If) {
            return self.parse_match();
        }

        let start = self.next().span;
        let condition = self.parse_expr()?;
        self.expect_token("`then`", |kind| matches!(kind, TokenKind::Then))?;
        let then_branch = self.parse_expr()?;
        self.expect_token("`else`", |kind| matches!(kind, TokenKind::Else))?;
        let else_branch = self.parse_expr()?;
        let span = start.join(else_branch.span);

        Ok(Expr {
            kind: ExprKind::If {
                condition: Box::new(condition),
                then_branch: Box::new(then_branch),
                else_branch: Box::new(else_branch),
            },
            span,
        })
    }

    fn parse_match(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Match) {
            return self.parse_fn();
        }

        let start = self.next().span;
        let scrutinee = self.parse_expr()?;
        self.expect_token("`{`", |kind| matches!(kind, TokenKind::LBrace))?;
        let mut branches = Vec::new();

        while !matches!(self.peek().kind, TokenKind::RBrace) {
            branches.push(self.parse_match_branch()?);
            if matches!(self.peek().kind, TokenKind::Semicolon) {
                self.next();
            } else {
                break;
            }
        }

        let end = self.expect_token("`}`", |kind| matches!(kind, TokenKind::RBrace))?;
        Ok(Expr {
            kind: ExprKind::Match {
                scrutinee: Box::new(scrutinee),
                branches,
            },
            span: start.join(end),
        })
    }

    fn parse_match_branch(&mut self) -> Result<MatchBranch, SyntaxError> {
        let start = self.peek().span;
        let pattern = self.parse_pattern()?;
        self.expect_token("`=>`", |kind| matches!(kind, TokenKind::Arrow))?;
        let body = self.parse_expr()?;
        let span = start.join(body.span);

        Ok(MatchBranch {
            pattern,
            body,
            span,
        })
    }

    fn parse_pattern(&mut self) -> Result<ParsedPattern, SyntaxError> {
        let token = self.next();
        match token.kind {
            TokenKind::Underscore => Ok(ParsedPattern::Wildcard),
            TokenKind::Colon => {
                let (name, _) = self.expect_symbol_name()?;
                Ok(ParsedPattern::Symbol(Symbol::from(name)))
            }
            TokenKind::Ident(name) => {
                if matches!(self.peek().kind, TokenKind::LParen) {
                    self.next();
                    let mut fields = Vec::new();
                    if !matches!(self.peek().kind, TokenKind::RParen) {
                        loop {
                            fields.push(self.parse_pattern()?);
                            if matches!(self.peek().kind, TokenKind::Comma) {
                                self.next();
                                if matches!(self.peek().kind, TokenKind::RParen) {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;
                    Ok(ParsedPattern::Tagged {
                        tag: Symbol::from(name),
                        fields,
                    })
                } else {
                    Ok(ParsedPattern::Bind(name))
                }
            }
            TokenKind::True => Ok(ParsedPattern::Symbol(Symbol::from("true"))),
            TokenKind::False => Ok(ParsedPattern::Symbol(Symbol::from("false"))),
            other => Err(SyntaxError::new(
                self.source,
                token.span.start,
                format!("expected a pattern, found {}", other.describe()),
            )),
        }
    }

    fn parse_handler(&mut self) -> Result<Handler, SyntaxError> {
        let start = self.peek().span;
        let op = self.parse_op_name()?;
        self.expect_token("`(`", |kind| matches!(kind, TokenKind::LParen))?;
        let params = self.parse_ident_list(|kind| matches!(kind, TokenKind::RParen))?;
        self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;
        self.expect_token("`=>`", |kind| matches!(kind, TokenKind::Arrow))?;
        let body = self.parse_expr()?;
        let span = start.join(body.span);

        Ok(Handler {
            op,
            params,
            span,
            body,
        })
    }

    fn parse_fn(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Fn) {
            return self.parse_perform();
        }

        let start = self.next().span;
        self.expect_token("`(`", |kind| matches!(kind, TokenKind::LParen))?;
        let params = self.parse_ident_list(|kind| matches!(kind, TokenKind::RParen))?;
        self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;
        self.expect_token("`=>`", |kind| matches!(kind, TokenKind::Arrow))?;
        let body = self.parse_expr()?;
        let span = start.join(body.span);

        Ok(Expr {
            kind: ExprKind::Lambda {
                params,
                body: Box::new(body),
            },
            span,
        })
    }

    fn parse_perform(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Perform) {
            return self.parse_force();
        }

        let start = self.next().span;
        let op = self.parse_op_name()?;
        self.expect_token("`(`", |kind| matches!(kind, TokenKind::LParen))?;
        let args = self.parse_expr_list(|kind| matches!(kind, TokenKind::RParen))?;
        let end = self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;

        Ok(Expr {
            kind: ExprKind::Perform { op, args },
            span: start.join(end),
        })
    }

    fn parse_force(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Force) {
            return self.parse_call();
        }

        let start = self.next().span;
        let value = self.parse_force()?;
        Ok(Expr {
            kind: ExprKind::Force(Box::new(value.clone())),
            span: start.join(value.span),
        })
    }

    fn parse_call(&mut self) -> Result<Expr, SyntaxError> {
        let mut expr = self.parse_primary()?;

        loop {
            if !matches!(self.peek().kind, TokenKind::LParen) {
                return Ok(expr);
            }

            self.next();
            let args = self.parse_expr_list(|kind| matches!(kind, TokenKind::RParen))?;
            let end = self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;
            let span = expr.span.join(end);
            expr = Expr {
                kind: ExprKind::Call {
                    callee: Box::new(expr),
                    args,
                },
                span,
            };
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, SyntaxError> {
        let token = self.next();
        match token.kind {
            TokenKind::Ident(name) => Ok(Expr {
                kind: ExprKind::Name(name),
                span: token.span,
            }),
            TokenKind::Int(value) => Ok(Expr {
                kind: ExprKind::Integer(value),
                span: token.span,
            }),
            TokenKind::True => Ok(Expr {
                kind: ExprKind::Bool(true),
                span: token.span,
            }),
            TokenKind::False => Ok(Expr {
                kind: ExprKind::Bool(false),
                span: token.span,
            }),
            TokenKind::Colon => {
                let (name, end) = self.expect_symbol_name()?;
                Ok(Expr {
                    kind: ExprKind::Symbol(Symbol::from(name)),
                    span: token.span.join(end),
                })
            }
            TokenKind::String(bytes) => Ok(Expr {
                kind: ExprKind::Bytes(bytes),
                span: token.span,
            }),
            TokenKind::LParen => {
                let expr = self.parse_expr()?;
                self.expect_token("`)`", |kind| matches!(kind, TokenKind::RParen))?;
                Ok(expr)
            }
            TokenKind::LBracket => self.parse_list(token.span),
            TokenKind::LBrace => self.parse_record(token.span),
            TokenKind::Lazy => self.parse_lazy(token.span),
            other => Err(SyntaxError::new(
                self.source,
                token.span.start,
                format!("expected an expression, found {}", other.describe()),
            )),
        }
    }

    fn parse_list(&mut self, start: Span) -> Result<Expr, SyntaxError> {
        let items = self.parse_expr_list(|kind| matches!(kind, TokenKind::RBracket))?;
        let end = self.expect_token("`]`", |kind| matches!(kind, TokenKind::RBracket))?;
        Ok(Expr {
            kind: ExprKind::List(items),
            span: start.join(end),
        })
    }

    fn parse_record(&mut self, start: Span) -> Result<Expr, SyntaxError> {
        let mut fields = Vec::new();

        if !matches!(self.peek().kind, TokenKind::RBrace) {
            loop {
                let (name, _) = self.expect_ident()?;
                self.expect_token("`:`", |kind| matches!(kind, TokenKind::Colon))?;
                let value = self.parse_expr()?;
                fields.push((name, value));

                if matches!(self.peek().kind, TokenKind::Comma) {
                    self.next();
                    if matches!(self.peek().kind, TokenKind::RBrace) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        let end = self.expect_token("`}`", |kind| matches!(kind, TokenKind::RBrace))?;
        Ok(Expr {
            kind: ExprKind::Record(fields),
            span: start.join(end),
        })
    }

    fn parse_lazy(&mut self, start: Span) -> Result<Expr, SyntaxError> {
        self.expect_token("`{`", |kind| matches!(kind, TokenKind::LBrace))?;
        let body = self.parse_expr()?;
        let end = self.expect_token("`}`", |kind| matches!(kind, TokenKind::RBrace))?;
        Ok(Expr {
            kind: ExprKind::Lazy(Box::new(body)),
            span: start.join(end),
        })
    }

    fn parse_op_name(&mut self) -> Result<Symbol, SyntaxError> {
        let (head, _) = self.expect_ident()?;
        let mut name = head;

        while matches!(self.peek().kind, TokenKind::Dot) {
            self.next();
            let (segment, _) = self.expect_ident()?;
            name.push('.');
            name.push_str(&segment);
        }

        Ok(Symbol::from(name))
    }

    fn parse_ident_list<F>(&mut self, done: F) -> Result<Vec<String>, SyntaxError>
    where
        F: Fn(&TokenKind) -> bool,
    {
        let mut idents = Vec::new();

        if done(&self.peek().kind) {
            return Ok(idents);
        }

        loop {
            let (name, _) = self.expect_ident()?;
            idents.push(name);
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.next();
                if done(&self.peek().kind) {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(idents)
    }

    fn parse_expr_list<F>(&mut self, done: F) -> Result<Vec<Expr>, SyntaxError>
    where
        F: Fn(&TokenKind) -> bool,
    {
        let mut exprs = Vec::new();

        if done(&self.peek().kind) {
            return Ok(exprs);
        }

        loop {
            exprs.push(self.parse_expr()?);
            if matches!(self.peek().kind, TokenKind::Comma) {
                self.next();
                if done(&self.peek().kind) {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(exprs)
    }

    fn expect_ident(&mut self) -> Result<(String, Span), SyntaxError> {
        let token = self.next();
        match token.kind {
            TokenKind::Ident(name) => Ok((name, token.span)),
            TokenKind::Underscore => Ok(("_".to_string(), token.span)),
            other => Err(SyntaxError::new(
                self.source,
                token.span.start,
                format!("expected identifier, found {}", other.describe()),
            )),
        }
    }

    fn expect_symbol_name(&mut self) -> Result<(String, Span), SyntaxError> {
        let token = self.next();
        match token.kind {
            TokenKind::Ident(name) => Ok((name, token.span)),
            TokenKind::True => Ok(("true".to_string(), token.span)),
            TokenKind::False => Ok(("false".to_string(), token.span)),
            other => Err(SyntaxError::new(
                self.source,
                token.span.start,
                format!("expected symbol name, found {}", other.describe()),
            )),
        }
    }

    fn expect_token<F>(&mut self, expected: &'static str, matches: F) -> Result<Span, SyntaxError>
    where
        F: Fn(&TokenKind) -> bool,
    {
        let token = self.next();
        if matches(&token.kind) {
            Ok(token.span)
        } else {
            Err(SyntaxError::new(
                self.source,
                token.span.start,
                format!("expected {expected}, found {}", token.kind.describe()),
            ))
        }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.cursor]
    }

    fn next(&mut self) -> Token {
        let token = self.tokens[self.cursor].clone();
        if !matches!(token.kind, TokenKind::Eof) {
            self.cursor += 1;
        }
        token
    }

    fn error_here(&self, message: impl Into<String>) -> SyntaxError {
        SyntaxError::new(self.source, self.peek().span.start, message)
    }
}

fn lower_expr(source: &str, expr: &Expr, env: &mut Vec<String>) -> Result<Term, SyntaxError> {
    match &expr.kind {
        ExprKind::Name(name) => {
            let Some(index) = env.iter().rev().position(|binding| binding == name) else {
                return Err(SyntaxError::new(
                    source,
                    expr.span.start,
                    format!("unbound name `{name}`"),
                ));
            };
            Ok(Term::var(
                u32::try_from(index).expect("environment should fit into u32"),
            ))
        }
        ExprKind::Integer(value) => Ok(Term::Value(Value::Integer(*value))),
        ExprKind::Bytes(bytes) => Ok(Term::Value(Value::Bytes(bytes.clone()))),
        ExprKind::Bool(value) => Ok(Term::Value(Value::Symbol(Symbol::from(if *value {
            "true"
        } else {
            "false"
        })))),
        ExprKind::Symbol(symbol) => Ok(Term::Value(Value::Symbol(symbol.clone()))),
        ExprKind::List(_) | ExprKind::Record(_) => Ok(Term::Value(lower_value(source, expr)?)),
        ExprKind::Let { name, value, body } => {
            let lowered_value = lower_expr(source, value, env)?;
            env.push(name.clone());
            let lowered_body = lower_expr(source, body, env)?;
            env.pop();

            Ok(Term::Apply {
                callee: Box::new(Term::lambda(1, lowered_body)),
                args: vec![lowered_value],
            })
        }
        ExprKind::LetRec { bindings, body } => {
            let mut seen = BTreeSet::new();
            for binding in bindings {
                if !seen.insert(binding.name.clone()) {
                    return Err(SyntaxError::new(
                        source,
                        binding.span.start,
                        format!("duplicate recursive binding `{}`", binding.name),
                    ));
                }
            }

            env.extend(bindings.iter().map(|binding| binding.name.clone()));
            let lowered_bindings = bindings
                .iter()
                .map(|binding| lower_rec_binding(source, binding, env))
                .collect::<Result<Vec<_>, _>>()?;
            let lowered_body = lower_expr(source, body, env)?;
            env.truncate(env.len() - bindings.len());

            Ok(Term::Rec {
                bindings: lowered_bindings,
                body: Box::new(lowered_body),
            })
        }
        ExprKind::Lambda { params, body } => {
            let arity = u16::try_from(params.len()).map_err(|_| {
                SyntaxError::new(source, expr.span.start, "function has too many parameters")
            })?;
            env.extend(params.iter().cloned());
            let lowered_body = lower_expr(source, body, env)?;
            env.truncate(env.len() - params.len());
            Ok(Term::lambda(arity, lowered_body))
        }
        ExprKind::Call { callee, args } => Ok(Term::Apply {
            callee: Box::new(lower_expr(source, callee, env)?),
            args: args
                .iter()
                .map(|arg| lower_expr(source, arg, env))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        ExprKind::Perform { op, args } => Ok(Term::Perform {
            op: op.clone(),
            args: args
                .iter()
                .map(|arg| lower_expr(source, arg, env))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        ExprKind::Handle { body, handlers } => {
            let mut lowered_handlers = BTreeMap::new();
            for handler in handlers {
                if lowered_handlers.contains_key(&handler.op) {
                    return Err(SyntaxError::new(
                        source,
                        handler.span.start,
                        format!("duplicate handler for `{}`", handler.op),
                    ));
                }
                lowered_handlers.insert(handler.op.clone(), lower_handler(source, handler, env)?);
            }

            Ok(Term::Handle {
                body: Box::new(lower_expr(source, body, env)?),
                handlers: lowered_handlers,
            })
        }
        ExprKind::Match {
            scrutinee,
            branches,
        } => Ok(Term::Case {
            scrutinee: Box::new(lower_expr(source, scrutinee, env)?),
            branches: branches
                .iter()
                .map(|branch| lower_match_branch(source, branch, env))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        ExprKind::If {
            condition,
            then_branch,
            else_branch,
        } => Ok(Term::Case {
            scrutinee: Box::new(lower_expr(source, condition, env)?),
            branches: vec![
                CaseBranch::new(
                    Pattern::Symbol(Symbol::from("true")),
                    lower_expr(source, then_branch, env)?,
                ),
                CaseBranch::new(
                    Pattern::Symbol(Symbol::from("false")),
                    lower_expr(source, else_branch, env)?,
                ),
            ],
        }),
        ExprKind::Lazy(body) => Ok(thunk::delay(lower_expr(source, body, env)?)),
        ExprKind::Force(value) => Ok(thunk::force(lower_expr(source, value, env)?)),
    }
}

fn lower_rec_binding(
    source: &str,
    binding: &RecExprBinding,
    env: &mut Vec<String>,
) -> Result<RecBinding, SyntaxError> {
    let arity = u16::try_from(binding.params.len()).map_err(|_| {
        SyntaxError::new(
            source,
            binding.span.start,
            "recursive function has too many parameters",
        )
    })?;
    env.extend(binding.params.iter().cloned());
    let body = lower_expr(source, &binding.body, env)?;
    env.truncate(env.len() - binding.params.len());
    Ok(RecBinding::new(Lambda::new(arity, body)))
}

fn lower_match_branch(
    source: &str,
    branch: &MatchBranch,
    env: &mut Vec<String>,
) -> Result<CaseBranch, SyntaxError> {
    let mut names = Vec::new();
    branch.pattern.binding_names(&mut names);
    reject_duplicate_pattern_names(source, branch.span.start, &names)?;

    env.extend(names);
    let body = lower_expr(source, &branch.body, env)?;
    env.truncate(env.len() - branch.pattern.bindings_usize());

    Ok(CaseBranch::new(lower_pattern(&branch.pattern), body))
}

fn reject_duplicate_pattern_names(
    source: &str,
    offset: usize,
    names: &[String],
) -> Result<(), SyntaxError> {
    let mut seen = BTreeSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(SyntaxError::new(
                source,
                offset,
                format!("duplicate pattern binding `{name}`"),
            ));
        }
    }
    Ok(())
}

fn lower_pattern(pattern: &ParsedPattern) -> Pattern {
    match pattern {
        ParsedPattern::Wildcard => Pattern::Wildcard,
        ParsedPattern::Bind(_) => Pattern::Bind,
        ParsedPattern::Symbol(symbol) => Pattern::Symbol(symbol.clone()),
        ParsedPattern::Tagged { tag, fields } => Pattern::Tagged {
            tag: tag.clone(),
            fields: fields.iter().map(lower_pattern).collect(),
        },
    }
}

impl ParsedPattern {
    fn bindings_usize(&self) -> usize {
        match self {
            Self::Wildcard | Self::Symbol(_) => 0,
            Self::Bind(_) => 1,
            Self::Tagged { fields, .. } => fields.iter().map(Self::bindings_usize).sum(),
        }
    }
}

fn lower_handler(
    source: &str,
    handler: &Handler,
    env: &mut Vec<String>,
) -> Result<Term, SyntaxError> {
    let arity = u16::try_from(handler.params.len()).map_err(|_| {
        SyntaxError::new(
            source,
            handler.span.start,
            "handler has too many parameters",
        )
    })?;

    env.extend(handler.params.iter().cloned());
    let body = lower_expr(source, &handler.body, env)?;
    env.truncate(env.len() - handler.params.len());
    Ok(Term::lambda(arity, body))
}

fn lower_value(source: &str, expr: &Expr) -> Result<Value, SyntaxError> {
    match &expr.kind {
        ExprKind::Integer(value) => Ok(Value::Integer(*value)),
        ExprKind::Bytes(bytes) => Ok(Value::Bytes(bytes.clone())),
        ExprKind::Bool(value) => Ok(Value::Symbol(Symbol::from(if *value {
            "true"
        } else {
            "false"
        }))),
        ExprKind::Symbol(symbol) => Ok(Value::Symbol(symbol.clone())),
        ExprKind::List(items) => Ok(Value::List(
            items
                .iter()
                .map(|item| lower_value(source, item))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        ExprKind::Record(fields) => {
            let mut entries = BTreeMap::new();
            for (name, value) in fields {
                let key = Symbol::from(name.clone());
                if entries.contains_key(&key) {
                    return Err(SyntaxError::new(
                        source,
                        expr.span.start,
                        format!("duplicate record field `{name}`"),
                    ));
                }
                entries.insert(key, lower_value(source, value)?);
            }
            Ok(Value::Record(entries))
        }
        _ => Err(SyntaxError::new(
            source,
            expr.span.start,
            format!(
                "{} cannot appear inside a data literal yet",
                literal_restriction_name(expr)
            ),
        )),
    }
}

fn literal_restriction_name(expr: &Expr) -> &'static str {
    match expr.kind {
        ExprKind::Name(_) => "a variable reference",
        ExprKind::Let { .. } => "a let expression",
        ExprKind::LetRec { .. } => "a recursive let expression",
        ExprKind::Lambda { .. } => "a function",
        ExprKind::Call { .. } => "a function call",
        ExprKind::Perform { .. } => "an effect request",
        ExprKind::Handle { .. } => "a handler block",
        ExprKind::Match { .. } => "a match expression",
        ExprKind::If { .. } => "an if expression",
        ExprKind::Lazy(_) => "a lazy block",
        ExprKind::Force(_) => "a force expression",
        ExprKind::Integer(_)
        | ExprKind::Bytes(_)
        | ExprKind::Bool(_)
        | ExprKind::Symbol(_)
        | ExprKind::List(_)
        | ExprKind::Record(_) => "this expression",
    }
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn line_and_column(source: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut column = 1;

    for ch in source[..offset].chars() {
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    (line, column)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvalResult, RuntimeValue, eval};

    #[test]
    fn parses_and_runs_let_lambda_calls() {
        let term = parse("let id = fn(x) => x; id(7)").expect("program should parse");
        let result = eval(term).expect("program should evaluate");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 7),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn parses_handlers_and_continuations() {
        let term = parse("handle perform ask(41) with { ask(value, resume) => resume(value) }")
            .expect("program should parse");
        let result = eval(term).expect("program should evaluate");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 41),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn lowers_lazy_and_force_through_thunks() {
        let term = parse("force lazy { 9 }").expect("program should parse");
        let result = eval(term).expect("program should evaluate");

        match result {
            EvalResult::Yielded(yielded) => {
                assert_eq!(yielded.op, Symbol::from(thunk::FORCE_OP));
            }
            other => panic!("expected thunk force yield, got {other:?}"),
        }
    }

    #[test]
    fn parses_list_and_record_literals() {
        let term = parse("{ items: [1, 2], message: \"ok\" }").expect("literal should parse");

        match term {
            Term::Value(Value::Record(entries)) => {
                assert_eq!(
                    entries.get(&Symbol::from("items")),
                    Some(&Value::List(vec![Value::Integer(1), Value::Integer(2)]))
                );
                assert_eq!(
                    entries.get(&Symbol::from("message")),
                    Some(&Value::Bytes(b"ok".to_vec()))
                );
            }
            other => panic!("unexpected term: {other:?}"),
        }
    }

    #[test]
    fn rejects_dynamic_values_inside_records_for_now() {
        let error = parse("{ value: x }").expect_err("record should reject variable values");
        assert!(
            error
                .to_string()
                .contains("variable reference cannot appear inside a data literal yet")
        );
    }
}
