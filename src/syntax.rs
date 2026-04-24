use std::collections::BTreeMap;
use std::fmt;

use crate::{Symbol, Term, Value, thunk};

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
    Arrow,
    Let,
    Fn,
    Perform,
    Handle,
    With,
    Lazy,
    Force,
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
            Self::Arrow => "`=>`".to_string(),
            Self::Let => "`let`".to_string(),
            Self::Fn => "`fn`".to_string(),
            Self::Perform => "`perform`".to_string(),
            Self::Handle => "`handle`".to_string(),
            Self::With => "`with`".to_string(),
            Self::Lazy => "`lazy`".to_string(),
            Self::Force => "`force`".to_string(),
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
    List(Vec<Expr>),
    Record(Vec<(String, Expr)>),
    Let {
        name: String,
        value: Box<Expr>,
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
    Perform {
        op: Symbol,
        args: Vec<Expr>,
    },
    Handle {
        body: Box<Expr>,
        handlers: Vec<Handler>,
    },
    Lazy(Box<Expr>),
    Force(Box<Expr>),
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
                } else {
                    self.offset += 1;
                    TokenKind::Equal
                }
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
            "fn" => TokenKind::Fn,
            "perform" => TokenKind::Perform,
            "handle" => TokenKind::Handle,
            "with" => TokenKind::With,
            "lazy" => TokenKind::Lazy,
            "force" => TokenKind::Force,
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

    fn parse_handle(&mut self) -> Result<Expr, SyntaxError> {
        if !matches!(self.peek().kind, TokenKind::Handle) {
            return self.parse_fn();
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
            other => Err(SyntaxError::new(
                self.source,
                token.span.start,
                format!("expected identifier, found {}", other.describe()),
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
        ExprKind::Lazy(body) => Ok(thunk::delay(lower_expr(source, body, env)?)),
        ExprKind::Force(value) => Ok(thunk::force(lower_expr(source, value, env)?)),
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
        ExprKind::Lambda { .. } => "a function",
        ExprKind::Call { .. } => "a function call",
        ExprKind::Perform { .. } => "an effect request",
        ExprKind::Handle { .. } => "a handler block",
        ExprKind::Lazy(_) => "a lazy block",
        ExprKind::Force(_) => "a force expression",
        ExprKind::Integer(_) | ExprKind::Bytes(_) | ExprKind::List(_) | ExprKind::Record(_) => {
            "this expression"
        }
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
