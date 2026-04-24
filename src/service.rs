use std::collections::BTreeMap;

use crate::effects::{clock, process};
use crate::{Symbol, Term};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Service {
    name: Symbol,
    spec: ServiceSpec,
}

impl Service {
    pub fn new(name: impl Into<Symbol>, spec: ServiceSpec) -> Self {
        Self {
            name: name.into(),
            spec,
        }
    }

    pub fn name(&self) -> &Symbol {
        &self.name
    }

    pub fn spec(&self) -> &ServiceSpec {
        &self.spec
    }

    pub fn to_spawn_request(&self) -> process::SpawnRequest {
        self.spec.to_spawn_request()
    }

    pub fn start(&self) -> Term {
        self.spec.clone().into_term()
    }

    pub fn restart_decision(
        &self,
        status: &process::ProcessStatus,
        restart_count: u32,
    ) -> RestartDecision {
        self.spec.restart_decision(status, restart_count)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSpec {
    argv: Vec<Vec<u8>>,
    env_mode: process::EnvMode,
    env: BTreeMap<Symbol, Vec<u8>>,
    cwd: Option<Vec<u8>>,
    stdin: Vec<u8>,
    declared_inputs: Vec<Vec<u8>>,
    declared_outputs: Vec<Vec<u8>>,
    restart_policy: RestartPolicy,
}

impl ServiceSpec {
    pub fn new(argv: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            argv: argv.into_iter().collect(),
            env_mode: process::EnvMode::Clear,
            env: BTreeMap::new(),
            cwd: None,
            stdin: Vec::new(),
            declared_inputs: Vec::new(),
            declared_outputs: Vec::new(),
            restart_policy: RestartPolicy::never(),
        }
    }

    pub fn argv(&self) -> &[Vec<u8>] {
        &self.argv
    }

    pub fn restart_policy(&self) -> RestartPolicy {
        self.restart_policy
    }

    pub fn inherit_env(mut self) -> Self {
        self.env_mode = process::EnvMode::Inherit;
        self
    }

    pub fn cwd(mut self, cwd: impl Into<Vec<u8>>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
        self.stdin = stdin.into();
        self
    }

    pub fn env(mut self, name: impl Into<Symbol>, value: impl Into<Vec<u8>>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    pub fn declared_input(mut self, path: impl Into<Vec<u8>>) -> Self {
        self.declared_inputs.push(path.into());
        self
    }

    pub fn declared_output(mut self, path: impl Into<Vec<u8>>) -> Self {
        self.declared_outputs.push(path.into());
        self
    }

    pub fn with_restart_policy(mut self, restart_policy: RestartPolicy) -> Self {
        self.restart_policy = restart_policy;
        self
    }

    pub fn to_spawn_request(&self) -> process::SpawnRequest {
        let mut request = process::SpawnRequest::new(self.argv.clone());
        if self.env_mode == process::EnvMode::Inherit {
            request = request.inherit_env();
        }
        if let Some(cwd) = &self.cwd {
            request = request.cwd(cwd.clone());
        }
        if !self.stdin.is_empty() {
            request = request.stdin(self.stdin.clone());
        }
        for (name, value) in &self.env {
            request = request.env(name.clone(), value.clone());
        }
        for path in &self.declared_inputs {
            request = request.declared_input(path.clone());
        }
        for path in &self.declared_outputs {
            request = request.declared_output(path.clone());
        }
        request
    }

    pub fn into_term(self) -> Term {
        self.to_spawn_request().into_term()
    }

    pub fn restart_decision(
        &self,
        status: &process::ProcessStatus,
        restart_count: u32,
    ) -> RestartDecision {
        self.restart_policy.decision(status, restart_count)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RestartMode {
    #[default]
    Never,
    Always,
    OnFailure,
}

impl RestartMode {
    fn should_restart(self, status: &process::ProcessStatus) -> bool {
        match self {
            Self::Never => false,
            Self::Always => true,
            Self::OnFailure => !matches!(status, process::ProcessStatus::ExitCode(0)),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RestartDelay {
    #[default]
    Immediate,
    Fixed {
        delay_nanos: i64,
    },
}

impl RestartDelay {
    pub fn sleep_request(self) -> Option<clock::SleepRequest> {
        match self {
            Self::Immediate => None,
            Self::Fixed { delay_nanos } => Some(clock::SleepRequest {
                duration_nanos: delay_nanos,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RestartPolicy {
    pub mode: RestartMode,
    pub max_restarts: Option<u32>,
    pub delay: RestartDelay,
}

impl RestartPolicy {
    pub fn never() -> Self {
        Self::default()
    }

    pub fn always() -> Self {
        Self {
            mode: RestartMode::Always,
            ..Self::default()
        }
    }

    pub fn on_failure() -> Self {
        Self {
            mode: RestartMode::OnFailure,
            ..Self::default()
        }
    }

    pub fn with_max_restarts(mut self, max_restarts: u32) -> Self {
        self.max_restarts = Some(max_restarts);
        self
    }

    pub fn with_delay(mut self, delay: RestartDelay) -> Self {
        self.delay = delay;
        self
    }

    pub fn decision(self, status: &process::ProcessStatus, restart_count: u32) -> RestartDecision {
        if !self.mode.should_restart(status) {
            return RestartDecision::Stop;
        }

        if let Some(max_restarts) = self.max_restarts {
            if restart_count >= max_restarts {
                return RestartDecision::Stop;
            }
        }

        match self.delay.sleep_request() {
            None => RestartDecision::RestartNow,
            Some(delay) => RestartDecision::RestartAfter(delay),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestartDecision {
    Stop,
    RestartNow,
    RestartAfter(clock::SleepRequest),
}

impl RestartDecision {
    pub fn delay_term(&self) -> Option<Term> {
        match self {
            Self::RestartAfter(request) => Some(request.clone().into_term()),
            Self::Stop | Self::RestartNow => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_spec_builds_process_requests_explicitly() {
        let spec = ServiceSpec::new(vec![b"/bin/server".to_vec(), b"--serve".to_vec()])
            .inherit_env()
            .cwd(b"/srv/app".to_vec())
            .stdin(b"config".to_vec())
            .env("PORT", b"8080".to_vec())
            .declared_input(b"/etc/service.conf".to_vec())
            .declared_output(b"/var/log/service.log".to_vec())
            .with_restart_policy(
                RestartPolicy::on_failure()
                    .with_max_restarts(3)
                    .with_delay(RestartDelay::Fixed { delay_nanos: 25 }),
            );

        let request = spec.to_spawn_request();

        assert_eq!(
            request.argv,
            vec![b"/bin/server".to_vec(), b"--serve".to_vec()]
        );
        assert_eq!(request.env_mode, process::EnvMode::Inherit);
        assert_eq!(request.cwd, Some(b"/srv/app".to_vec()));
        assert_eq!(request.stdin, b"config".to_vec());
        assert_eq!(
            request.env.get(&Symbol::from("PORT")),
            Some(&b"8080".to_vec())
        );
        assert_eq!(request.declared_inputs, vec![b"/etc/service.conf".to_vec()]);
        assert_eq!(
            request.declared_outputs,
            vec![b"/var/log/service.log".to_vec()]
        );
        assert_eq!(
            spec.restart_policy(),
            RestartPolicy::on_failure()
                .with_max_restarts(3)
                .with_delay(RestartDelay::Fixed { delay_nanos: 25 })
        );
    }

    #[test]
    fn service_start_delegates_to_process_spawn_term() {
        let service = Service::new(
            "api",
            ServiceSpec::new(vec![b"/bin/api".to_vec()]).declared_output(b"/tmp/api.pid".to_vec()),
        );

        assert_eq!(service.start(), service.to_spawn_request().into_term());
    }

    #[test]
    fn restart_policy_stops_after_success_for_on_failure_mode() {
        let decision =
            RestartPolicy::on_failure().decision(&process::ProcessStatus::ExitCode(0), 0);

        assert_eq!(decision, RestartDecision::Stop);
    }

    #[test]
    fn restart_policy_restarts_failures_after_clock_delay() {
        let decision = RestartPolicy::on_failure()
            .with_delay(RestartDelay::Fixed { delay_nanos: 500 })
            .decision(&process::ProcessStatus::Signal(9), 1);

        assert_eq!(
            decision,
            RestartDecision::RestartAfter(clock::SleepRequest {
                duration_nanos: 500,
            })
        );
        assert_eq!(decision.delay_term(), Some(clock::sleep(500)));
    }

    #[test]
    fn restart_policy_honors_restart_limit() {
        let policy = RestartPolicy::always().with_max_restarts(2);

        assert_eq!(
            policy.decision(&process::ProcessStatus::ExitCode(0), 1),
            RestartDecision::RestartNow
        );
        assert_eq!(
            policy.decision(&process::ProcessStatus::ExitCode(0), 2),
            RestartDecision::Stop
        );
    }

    #[test]
    fn service_uses_service_level_restart_policy() {
        let service = Service::new(
            "worker",
            ServiceSpec::new(vec![b"/bin/worker".to_vec()])
                .with_restart_policy(RestartPolicy::always().with_max_restarts(1)),
        );

        assert_eq!(
            service.restart_decision(&process::ProcessStatus::Unknown, 0),
            RestartDecision::RestartNow
        );
        assert_eq!(
            service.restart_decision(&process::ProcessStatus::Unknown, 1),
            RestartDecision::Stop
        );
    }
}
