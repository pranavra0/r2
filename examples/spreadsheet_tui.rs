use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use crossterm::{execute, terminal::EnterAlternateScreen, terminal::LeaveAlternateScreen};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame, Terminal,
};
use r2::{CellId, FailureKind, ForceResult, HostFn, Outcome, Runtime, Value};
use std::collections::BTreeMap;
use std::io;

// ─────────────────────────────────────────────
// Formula parser
// ─────────────────────────────────────────────

#[derive(Debug)]
enum Expr {
    Number(i64),
    CellRef { col: u16, row: u16 },
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}

fn parse_formula(input: &str) -> anyhow::Result<Expr> {
    let mut p = Parser { input, pos: 0 };
    let expr = p.expr()?;
    p.skip_ws();
    if !p.eof() {
        anyhow::bail!("unexpected '{}'", p.peek().unwrap());
    }
    Ok(expr)
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn expr(&mut self) -> anyhow::Result<Expr> {
        let mut left = self.term()?;
        loop {
            self.skip_ws();
            match self.peek() {
                Some('+') => {
                    self.advance();
                    let right = self.term()?;
                    left = Expr::Add(Box::new(left), Box::new(right));
                }
                Some('-') => {
                    self.advance();
                    let right = self.term()?;
                    left = Expr::Sub(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn term(&mut self) -> anyhow::Result<Expr> {
        let mut left = self.factor()?;
        loop {
            self.skip_ws();
            match self.peek() {
                Some('*') => {
                    self.advance();
                    let right = self.factor()?;
                    left = Expr::Mul(Box::new(left), Box::new(right));
                }
                Some('/') => {
                    self.advance();
                    let right = self.factor()?;
                    left = Expr::Div(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn factor(&mut self) -> anyhow::Result<Expr> {
        self.skip_ws();
        if self.consume('(') {
            let e = self.expr()?;
            self.skip_ws();
            if !self.consume(')') {
                anyhow::bail!("expected ')'");
            }
            return Ok(e);
        }
        if let Some(cell) = self.parse_cell_ref() {
            return Ok(Expr::CellRef {
                col: cell.0,
                row: cell.1,
            });
        }
        if let Some(n) = self.parse_number() {
            return Ok(Expr::Number(n));
        }
        anyhow::bail!("expected number, cell ref, or '(' at pos {}", self.pos)
    }

    fn parse_cell_ref(&mut self) -> Option<(u16, u16)> {
        let start = self.pos;
        let col_chars: String = self
            .input[start..]
            .chars()
            .take_while(|c| c.is_ascii_uppercase())
            .collect();
        if col_chars.is_empty() || col_chars.len() > 1 {
            return None;
        }
        let col = (col_chars.as_bytes()[0] - b'A') as u16;
        self.pos += 1;

        let row_start = self.pos;
        let row_chars: String = self
            .input[row_start..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if row_chars.is_empty() {
            self.pos = start;
            return None;
        }
        let row: u16 = row_chars.parse().ok()?;
        if row == 0 {
            self.pos = start;
            return None;
        }
        self.pos += row_chars.len();
        Some((col, row - 1))
    }

    fn parse_number(&mut self) -> Option<i64> {
        let start = self.pos;
        let digits: String = self
            .input[start..]
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '-')
            .collect();
        if digits.is_empty() || digits == "-" {
            return None;
        }
        let n = digits.parse().ok()?;
        self.pos += digits.len();
        Some(n)
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek() {
            self.pos += c.len_utf8();
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.peek() == Some(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.input.len()
    }
}

// ─────────────────────────────────────────────
// Spreadsheet model backed by r2
// ─────────────────────────────────────────────

struct SheetCell {
    raw: String,
    cell_id: CellId,
}

struct Sheet {
    cells: BTreeMap<(u16, u16), SheetCell>,
    rt: Runtime,
}

impl Sheet {
    fn new(store_path: &str) -> anyhow::Result<Self> {
        let _ = std::fs::remove_dir_all(store_path);
        let mut rt = Runtime::new(store_path)?;

        rt.register(
            "+",
            HostFn::pure(|args| match args {
                [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a + b)),
                _ => Err(FailureKind::TypeError("+ expects two ints".into())),
            }),
        );
        rt.register(
            "-",
            HostFn::pure(|args| match args {
                [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a - b)),
                _ => Err(FailureKind::TypeError("- expects two ints".into())),
            }),
        );
        rt.register(
            "*",
            HostFn::pure(|args| match args {
                [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a * b)),
                _ => Err(FailureKind::TypeError("* expects two ints".into())),
            }),
        );
        rt.register(
            "/",
            HostFn::pure(|args| match args {
                [Value::Int(_), Value::Int(0)] => Ok(Value::Int(0)),
                [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a / b)),
                _ => Err(FailureKind::TypeError("/ expects two ints".into())),
            }),
        );

        let mut sheet = Self {
            cells: BTreeMap::new(),
            rt,
        };

        // Pre-populate a little demo data
        sheet.set_cell(0, 0, "100")?; // A1
        sheet.set_cell(0, 1, "10")?;  // A2
        sheet.set_cell(1, 0, "=A1*2")?; // B1
        sheet.set_cell(1, 1, "=A1+A2")?; // B2
        sheet.set_cell(2, 0, "=B1+B2")?; // C1

        Ok(sheet)
    }

    fn ensure_cell(&mut self, col: u16, row: u16) -> anyhow::Result<CellId> {
        if let Some(cell) = self.cells.get(&(col, row)) {
            Ok(cell.cell_id.clone())
        } else {
            let zero = self.rt.int(0)?;
            let cell_id = self.rt.cell_new(zero)?;
            self.cells.insert(
                (col, row),
                SheetCell {
                    raw: "0".into(),
                    cell_id: cell_id.clone(),
                },
            );
            Ok(cell_id)
        }
    }

    fn set_cell(&mut self, col: u16, row: u16, raw: &str) -> anyhow::Result<()> {
        let raw = raw.trim();
        let cell_id = self.ensure_cell(col, row)?;

        if raw.starts_with('=') {
            let expr = parse_formula(&raw[1..])?;
            let hash = self.compile_expr(&expr)?;
            let thunk = self.rt.thunk(hash)?;
            self.rt.cell_set(&cell_id, thunk)?;
        } else {
            let value: i64 = raw.parse()?;
            let hash = self.rt.int(value)?;
            self.rt.cell_set(&cell_id, hash)?;
        }

        self.cells.insert(
            (col, row),
            SheetCell {
                raw: raw.into(),
                cell_id,
            },
        );
        Ok(())
    }

    fn compile_expr(&mut self, expr: &Expr) -> anyhow::Result<r2::Hash> {
        match expr {
            Expr::Number(n) => self.rt.int(*n),
            Expr::CellRef { col, row } => {
                let id = self.ensure_cell(*col, *row)?;
                self.rt.read_cell(id)
            }
            Expr::Add(a, b) => {
                let left = self.compile_expr(a)?;
                let right = self.compile_expr(b)?;
                self.rt.call("+", vec![left, right])
            }
            Expr::Sub(a, b) => {
                let left = self.compile_expr(a)?;
                let right = self.compile_expr(b)?;
                self.rt.call("-", vec![left, right])
            }
            Expr::Mul(a, b) => {
                let left = self.compile_expr(a)?;
                let right = self.compile_expr(b)?;
                self.rt.call("*", vec![left, right])
            }
            Expr::Div(a, b) => {
                let left = self.compile_expr(a)?;
                let right = self.compile_expr(b)?;
                self.rt.call("/", vec![left, right])
            }
        }
    }

    fn force_cell(&self, col: u16, row: u16) -> anyhow::Result<(String, ForceResult)> {
        let cell = self
            .cells
            .get(&(col, row))
            .ok_or_else(|| anyhow::anyhow!("cell not found"))?;
        let current = self
            .rt
            .cell_current(&cell.cell_id)?
            .ok_or_else(|| anyhow::anyhow!("no current version"))?;
        let result = self.rt.force(current.value)?;
        let display = match &result.outcome {
            Outcome::Success(hash) => match self.rt.get_value(hash)? {
                Value::Int(n) => format!("{}", n),
                other => format!("{:?}", other),
            },
            Outcome::Failure(f) => format!("ERR: {}", f.kind),
        };
        Ok((display, result))
    }
}

// ─────────────────────────────────────────────
// TUI App
// ─────────────────────────────────────────────

enum Mode {
    Normal,
    Editing,
}

struct App {
    sheet: Sheet,
    cursor: (u16, u16),
    offset: (u16, u16),
    mode: Mode,
    edit_buffer: String,
    message: String,
}

impl App {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            sheet: Sheet::new(".r2")?,
            cursor: (0, 0),
            offset: (0, 0),
            mode: Mode::Normal,
            edit_buffer: String::new(),
            message: "Welcome to r2 spreadsheet!".into(),
        })
    }

    fn move_cursor(&mut self, dx: i16, dy: i16) {
        let new_col = (self.cursor.0 as i16 + dx).max(0) as u16;
        let new_row = (self.cursor.1 as i16 + dy).max(0) as u16;
        self.cursor = (new_col, new_row);
    }

    fn start_edit(&mut self) {
        let raw = self
            .sheet
            .cells
            .get(&self.cursor)
            .map(|c| c.raw.clone())
            .unwrap_or_default();
        self.edit_buffer = raw;
        self.mode = Mode::Editing;
    }

    fn commit_edit(&mut self) {
        let (col, row) = self.cursor;
        let raw = self.edit_buffer.clone();
        match self.sheet.set_cell(col, row, &raw) {
            Ok(()) => {
                self.message = format!("Set {}{} = {}", col_name(col), row + 1, raw);
            }
            Err(e) => {
                self.message = format!("Error: {}", e);
            }
        }
        self.mode = Mode::Normal;
    }

    fn cancel_edit(&mut self) {
        self.mode = Mode::Normal;
        self.message = "Cancelled".into();
    }
}

fn col_name(col: u16) -> String {
    ((b'A' + col as u8) as char).to_string()
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> anyhow::Result<()> {
    let mut app = App::new()?;

    loop {
        terminal.draw(|f| ui(f, &app))?;

        if !event::poll(std::time::Duration::from_millis(50))? {
            continue;
        }
        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            match app.mode {
                Mode::Normal => match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('e') | KeyCode::Enter => app.start_edit(),
                    KeyCode::Left => app.move_cursor(-1, 0),
                    KeyCode::Right => app.move_cursor(1, 0),
                    KeyCode::Up => app.move_cursor(0, -1),
                    KeyCode::Down => app.move_cursor(0, 1),
                    _ => {}
                },
                Mode::Editing => match key.code {
                    KeyCode::Enter => app.commit_edit(),
                    KeyCode::Esc => app.cancel_edit(),
                    KeyCode::Backspace => {
                        app.edit_buffer.pop();
                    }
                    KeyCode::Char(c) => {
                        app.edit_buffer.push(c);
                    }
                    _ => {}
                },
            }
        }
    }

    Ok(())
}

fn ui(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(10),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(frame.area());

    render_grid(frame, chunks[0], app);
    render_info(frame, chunks[1], app);
    render_bottom(frame, chunks[2], app);
}

fn render_grid(frame: &mut Frame, area: Rect, app: &App) {
    let inner_width = area.width.saturating_sub(2);
    let inner_height = area.height.saturating_sub(2);
    let cell_width: u16 = 10;
    let label_width: u16 = 3;
    let visible_cols = ((inner_width.saturating_sub(label_width)) / cell_width).max(1);
    let visible_rows = inner_height.saturating_sub(1).max(1);

    // Adjust offset to keep cursor visible
    let mut offset = app.offset;
    if app.cursor.0 < offset.0 {
        offset.0 = app.cursor.0;
    }
    if app.cursor.0 >= offset.0 + visible_cols {
        offset.0 = app.cursor.0 - visible_cols + 1;
    }
    if app.cursor.1 < offset.1 {
        offset.1 = app.cursor.1;
    }
    if app.cursor.1 >= offset.1 + visible_rows {
        offset.1 = app.cursor.1 - visible_rows + 1;
    }

    let mut lines = Vec::new();

    // Column header
    let mut header_spans = vec![Span::raw("   ")];
    for c in 0..visible_cols {
        header_spans.push(Span::styled(
            format!(" {:^8}", col_name(offset.0 + c)),
            Style::default().fg(Color::Yellow),
        ));
    }
    lines.push(Line::from(header_spans));

    // Rows
    for r in 0..visible_rows {
        let row_idx = offset.1 + r;
        let mut spans = vec![Span::styled(
            format!("{:>2} ", row_idx + 1),
            Style::default().fg(Color::Yellow),
        )];

        for c in 0..visible_cols {
            let col_idx = offset.0 + c;
            let pos = (col_idx, row_idx);
            let display = app
                .sheet
                .cells
                .get(&pos)
                .map(|cell| {
                    let mut s = cell.raw.clone();
                    if s.len() > 8 {
                        s.truncate(7);
                        s.push('…');
                    }
                    s
                })
                .unwrap_or_default();

            let is_cursor = app.cursor == pos;
            let text = format!(" {:^8} ", display);
            if is_cursor {
                spans.push(Span::styled(
                    text,
                    Style::default().bg(Color::Blue).fg(Color::White),
                ));
            } else {
                spans.push(Span::raw(text));
            }
        }
        lines.push(Line::from(spans));
    }

    let paragraph = Paragraph::new(Text::from(lines))
        .block(Block::default().title(" r2 Spreadsheet ").borders(Borders::ALL));
    frame.render_widget(paragraph, area);
}

fn render_info(frame: &mut Frame, area: Rect, app: &App) {
    let (col, row) = app.cursor;
    let mut text_lines = Vec::new();

    text_lines.push(Line::from(vec![
        Span::styled("Cell: ", Style::default().fg(Color::Cyan)),
        Span::raw(format!("{}{}", col_name(col), row + 1)),
    ]));

    let raw = app
        .sheet
        .cells
        .get(&(col, row))
        .map(|c| c.raw.clone())
        .unwrap_or_else(|| "(empty)".into());
    text_lines.push(Line::from(vec![
        Span::styled("Raw: ", Style::default().fg(Color::Cyan)),
        Span::raw(raw),
    ]));

    match app.sheet.force_cell(col, row) {
        Ok((value, result)) => {
            let cache_text = if result.cache_hit { "HIT" } else { "MISS" };
            let cache_style = if result.cache_hit {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Red)
            };
            text_lines.push(Line::from(vec![
                Span::styled("Value: ", Style::default().fg(Color::Cyan)),
                Span::styled(value, Style::default().fg(Color::Green)),
                Span::raw("  "),
                Span::styled("Cache: ", Style::default().fg(Color::Cyan)),
                Span::styled(cache_text, cache_style),
            ]));
        }
        Err(e) => {
            text_lines.push(Line::from(vec![
                Span::styled("Error: ", Style::default().fg(Color::Red)),
                Span::raw(e.to_string()),
            ]));
        }
    }

    text_lines.push(Line::from(vec![
        Span::styled("Msg: ", Style::default().fg(Color::Cyan)),
        Span::raw(&app.message),
    ]));

    let paragraph = Paragraph::new(Text::from(text_lines))
        .block(Block::default().title(" Info ").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_bottom(frame: &mut Frame, area: Rect, app: &App) {
    match app.mode {
        Mode::Normal => {
            let help =
                " q:quit | e/Enter:edit | arrows:move ";
            let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
            frame.render_widget(paragraph, area);
        }
        Mode::Editing => {
            let text = format!("Edit: {} █", app.edit_buffer);
            let paragraph =
                Paragraph::new(text).style(Style::default().fg(Color::Yellow).bg(Color::Black));
            frame.render_widget(paragraph, area);
        }
    }
}

fn main() -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = run_app(&mut terminal);

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    if let Err(e) = res {
        eprintln!("Error: {e:?}");
    }

    Ok(())
}
