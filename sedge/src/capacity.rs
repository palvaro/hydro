//! Capacity from the outside: resource limits on each deployed process, changed mid-run for a
//! capacity-dip trigger, with the kernel's own accounting read back as ground truth.
//!
//! A program under Sedge used to model its own capacity: a per-tick budget, a `service_time`
//! spin-loop, a filler channel. Those knobs only exist to make overload reachable, and they put
//! the bottleneck inside the code under test. Here the bottleneck is put *around* the code
//! instead. Every deployed node of the program runs in its own container (or cgroup) with a CFS
//! CPU quota, and the program's capacity is whatever its real code achieves under that quota:
//! serialization, syscalls, Tokio scheduling and the handler itself all count.
//!
//! # Pieces
//!
//! - [`CpuQuota`] and [`Limits`]: what a node is allowed. The quota is a CFS `quota/period` pair;
//!   keep the period short (5–10 ms), because with the default 100 ms period a 20 % quota runs the
//!   process for 20 ms and then stops it for 80 ms, longer than a Hydro tick, which shows up as a
//!   burst of missed timeouts rather than as a slower server.
//! - [`CapacityBackend`]: how limits are applied. [`DockerBackend`] is the one that works on hosts
//!   without user cgroup delegation (cgroup v1, no `systemd-run --user`); it runs each node with
//!   `docker run --network host --cpu-period P --cpu-quota Q`, changes the quota live with
//!   `docker update`, and reads `cpu.stat` from the container's cgroup.
//! - [`ContainerHost`]: a `hydro_deploy` [`Host`] that wraps the deployment's [`LocalhostHost`]
//!   and differs from it only in [`LaunchedHost::launch_binary`], where the binary is replaced by
//!   a shim that the backend writes (for Docker, `exec docker run … <real binary> "$@"`). It keeps
//!   the localhost's id, so hydro_deploy still connects nodes over Unix sockets in `$TMPDIR` (which
//!   the container mounts) or localhost TCP (which `--network host` keeps working).
//! - [`Handle`]: one launched node, by name, role and member, which the harness uses to sample
//!   `cpu.stat` every second, to apply a [`CpuDip`], and to remove the container when the trial
//!   ends, including on panic (see [`Nodes`]).
//!
//! Only the program's own nodes are capped. The harness's relay process, the load generator and
//! the observer are not, and with a [`CapacityConfig::harness_cpuset`] they are pinned away from
//! the nodes' [`CapacityConfig::node_cpuset`], so that the harness never competes with the program
//! for CPU.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};

use hydro_deploy::localhost::LocalhostHost;
use hydro_deploy::rust_crate::build::BuildOutput;
use hydro_deploy::rust_crate::tracing_options::TracingOptions;
use hydro_deploy::{
    BaseServerStrategy, ClientStrategy, Host, HostStrategyGetter, HostTargetType, LaunchedBinary,
    LaunchedHost, PortNetworkHint, ResourceBatch, ResourceResult,
};
use hydro_deploy_integration::ServerBindConfig;

/// A CFS bandwidth limit: the node may run `quota_us` of CPU time (summed over its threads) in
/// every `period_us`. `quota_us / period_us` is the number of cores' worth of time it gets.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuQuota {
    pub quota_us: u64,
    pub period_us: u64,
}

impl CpuQuota {
    /// `cores` cores' worth of CPU per `period_us` (at least 1 ms of quota, the kernel's minimum).
    pub fn from_cores(cores: f64, period_us: u64) -> Self {
        Self {
            quota_us: ((cores * period_us as f64).round() as u64).max(1000),
            period_us,
        }
    }

    /// Cores' worth of CPU this quota gives.
    pub fn cores(&self) -> f64 {
        self.quota_us as f64 / self.period_us as f64
    }

    /// This quota divided by `k` (a dip), keeping the period, never below the kernel's 1 ms
    /// minimum.
    pub fn divided(&self, k: f64) -> Self {
        Self {
            quota_us: ((self.quota_us as f64 / k).round() as u64).max(1000),
            period_us: self.period_us,
        }
    }
}

impl fmt::Display for CpuQuota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:.3} cpu ({}us/{}us)",
            self.cores(),
            self.quota_us,
            self.period_us
        )
    }
}

/// The resources one node is given.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Limits {
    /// CPU bandwidth; `None` leaves the node uncapped (it still runs in a container, and its
    /// usage is still recorded).
    pub cpu: Option<CpuQuota>,
    /// The cores the node may run on, in `taskset`/cpuset list syntax (`4-7`, `4,6`).
    pub cpuset: Option<String>,
    /// A memory limit in bytes.
    pub memory: Option<u64>,
}

impl fmt::Display for Limits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cpu {
            Some(q) => write!(f, "{q}")?,
            None => write!(f, "uncapped")?,
        }
        if let Some(c) = &self.cpuset {
            write!(f, " on cpus {c}")?;
        }
        if let Some(m) = self.memory {
            write!(f, " mem {m}B")?;
        }
        Ok(())
    }
}

/// Which deployed node a [`Handle`] is: the role is the location's marker type name (`Server`,
/// `Replica`), the member its index for a cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeSpec {
    pub role: String,
    pub member: Option<u32>,
}

impl fmt::Display for NodeSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.member {
            Some(m) => write!(f, "{}:{m}", self.role),
            None => write!(f, "{}", self.role),
        }
    }
}

/// The kernel's CPU accounting for one node, cumulative since it started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CpuStat {
    /// CPU time used, in nanoseconds.
    pub usage_ns: u64,
    /// CFS periods in which the node was runnable.
    pub nr_periods: u64,
    /// Of those, periods in which it hit its quota and was throttled.
    pub nr_throttled: u64,
    /// Total time spent throttled, in nanoseconds.
    pub throttled_ns: u64,
}

/// The kernel's per-process accounting for one node (summed over the processes in its
/// container), cumulative since each process started: bytes passed to `read`/`write` syscalls
/// (sockets included: every message a node actually hands to the kernel), syscall counts, and
/// resident memory. Read from `/proc/<pid>/io` and `/proc/<pid>/status` from *outside* the node,
/// so it is measured even when the node is too starved to report anything itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcIo {
    pub rchar: u64,
    pub wchar: u64,
    pub syscr: u64,
    pub syscw: u64,
    /// Resident set size now (not cumulative), in KiB.
    pub rss_kb: u64,
}

/// How a node's process ended, if it has: read once the run is over, so that a node that died
/// mid-run (killed at its memory limit, or crashed) invalidates the trial instead of reading as an
/// overloaded program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeExit {
    /// Killed by the kernel's OOM killer at the node's memory limit.
    pub oom_killed: bool,
    /// The process's exit code.
    pub exit_code: i64,
}

/// One launched node.
#[derive(Debug)]
pub struct Handle {
    /// The container's name, unique per run: `sedge-<pid>-<trial>-<role>[-<member>]`.
    pub name: String,
    pub node: NodeSpec,
    /// The limits it was started with.
    pub limits: Limits,
    /// The quota currently applied, which a dip changes.
    pub current_cpu: Mutex<Option<CpuQuota>>,
    /// The backend's own id for the node (Docker's container id), looked up once it runs.
    pub backend_id: OnceLock<String>,
}

/// How limits are applied to deployed nodes. Every method is synchronous and cheap enough to
/// call from the harness (a CLI call or a file read); the harness calls the slow ones on Tokio's
/// blocking pool.
pub trait CapacityBackend: Send + Sync + fmt::Debug {
    /// A short name for reports (`docker`).
    fn name(&self) -> &'static str;

    /// Writes the executable that launches `real_bin` under `h`'s limits, passing its arguments
    /// through, and returns its path. hydro_deploy runs this executable in place of the binary,
    /// with the binary's arguments, environment, stdin and stdout.
    fn launcher(&self, h: &Handle, real_bin: &Path) -> io::Result<PathBuf>;

    /// Changes the node's CPU quota while it runs (a dip, or the restore after one).
    fn set_cpu(&self, h: &Handle, quota: CpuQuota) -> Result<(), String>;

    /// The node's CPU accounting, or `None` if it cannot be read (yet).
    fn cpu_stat(&self, h: &Handle) -> Option<CpuStat>;

    /// Stops and removes the node, whether or not it is still running. Idempotent.
    fn remove(&self, h: &Handle);

    /// Removes every node this backend started whose name begins with `prefix`, for cleaning up
    /// after an earlier run that was killed.
    fn remove_prefix(&self, prefix: &str);

    /// `Some` if the node is no longer running: how it ended. `None` while it runs (or if it
    /// cannot be told).
    fn exit(&self, _h: &Handle) -> Option<NodeExit> {
        None
    }

    /// The host pids of the node's processes, or empty if they cannot be found (yet).
    fn pids(&self, _h: &Handle) -> Vec<u32> {
        Vec::new()
    }

    /// Starts a sampler of the nodes' `/proc` accounting ([`ProcIo`]), running outside the nodes
    /// on `cpuset`, or `None` if this backend has none.
    fn proc_sampler(&self, _name: &str, _dir: &Path, _cpuset: Option<&str>) -> Option<ProcSampler> {
        None
    }
}

/// A sampler of the nodes' `/proc/<pid>/io` and `/proc/<pid>/status`, outside the nodes.
///
/// The harness usually cannot read another process's `/proc/<pid>/io` (it is guarded by a
/// ptrace check, which fails across user namespaces), so the sampler is a small container in the
/// host's pid namespace, as the invoking user, pinned to the harness's cores. It reads the pid
/// lists the harness writes to `<dir>/pids` (one line per node: `index pid…`) and rewrites
/// `<dir>/snap` (one line per node: `index rchar wchar syscr syscw rss_kb`) four times a second.
/// It uses only shell builtins between snapshots, so it costs the harness's cores almost nothing,
/// and the nodes' cores nothing.
#[derive(Debug)]
pub struct ProcSampler {
    pub name: String,
    pub dir: PathBuf,
    last_pids: Mutex<String>,
}

impl ProcSampler {
    /// Writes the nodes' pid lists (index = position in `pids`), if they changed.
    pub fn set_pids(&self, pids: &[Vec<u32>]) {
        let text: String = pids
            .iter()
            .enumerate()
            .map(|(i, ps)| {
                let ps: Vec<String> = ps.iter().map(|p| p.to_string()).collect();
                format!("{i} {}\n", ps.join(" "))
            })
            .collect();
        let mut last = self.last_pids.lock().unwrap();
        if *last == text {
            return;
        }
        let tmp = self.dir.join("pids.tmp");
        if fs::write(&tmp, &text).is_ok() && fs::rename(&tmp, self.dir.join("pids")).is_ok() {
            *last = text;
        }
    }

    /// The latest snapshot, one entry per node index (`None` where the node has no sample).
    pub fn read(&self, nodes: usize) -> Vec<Option<ProcIo>> {
        let mut out = vec![None; nodes];
        let Ok(text) = fs::read_to_string(self.dir.join("snap")) else {
            return out;
        };
        for line in text.lines() {
            let v: Vec<u64> = line
                .split_whitespace()
                .filter_map(|f| f.parse().ok())
                .collect();
            if let [i, rchar, wchar, syscr, syscw, rss_kb] = v[..]
                && (i as usize) < nodes
            {
                out[i as usize] = Some(ProcIo {
                    rchar,
                    wchar,
                    syscr,
                    syscw,
                    rss_kb,
                });
            }
        }
        out
    }
}

impl Drop for ProcSampler {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// The sampler's loop: bash builtins only, except `mv` and `sleep`.
const PROC_SAMPLER_SCRIPT: &str = r#"dir=$1
while :; do
  out=""
  if [[ -f $dir/pids ]]; then
    while read -r idx rest; do
      r=0; w=0; sr=0; sw=0; rss=0
      for p in $rest; do
        while read -r k v; do
          case $k in
            rchar:) r=$((r+v));; wchar:) w=$((w+v));;
            syscr:) sr=$((sr+v));; syscw:) sw=$((sw+v));;
          esac
        done 2>/dev/null < /proc/$p/io
        while read -r k v u; do
          [[ $k == VmRSS: ]] && rss=$((rss+v))
        done 2>/dev/null < /proc/$p/status
      done
      out+="$idx $r $w $sr $sw $rss"$'
'
    done < "$dir/pids"
  fi
  printf '%s' "$out" > "$dir/snap.tmp" && mv -f "$dir/snap.tmp" "$dir/snap"
  sleep 0.25
done
"#;

/// Runs each node in a Docker container on the host network, as the invoking user, with the
/// binary, its shared libraries and `$TMPDIR` bind-mounted at their own paths.
#[derive(Debug, Clone)]
pub struct DockerBackend {
    /// The image to run the binary in. Its glibc must be at least as new as the host's, since the
    /// binary is built on the host.
    pub image: String,
    /// Where the launcher scripts are written.
    pub shim_dir: PathBuf,
}

impl DockerBackend {
    pub fn new(image: impl Into<String>, shim_dir: impl Into<PathBuf>) -> Self {
        Self {
            image: image.into(),
            shim_dir: shim_dir.into(),
        }
    }

    /// Whether `docker` is on the path and its daemon answers.
    pub fn available() -> bool {
        Command::new("docker")
            .args(["version", "--format", "{{.Server.Version}}"])
            .output()
            .is_ok_and(|o| o.status.success())
    }

    fn container_id(&self, h: &Handle) -> Option<String> {
        if let Some(id) = h.backend_id.get() {
            return Some(id.clone());
        }
        let out = Command::new("docker")
            .args(["inspect", "-f", "{{.Id}}", &h.name])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let id = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        if id.is_empty() {
            return None;
        }
        let _ = h.backend_id.set(id.clone());
        Some(id)
    }
}

/// The invoking user's `uid:gid`, so that files the node writes in `$TMPDIR` (its Unix sockets)
/// belong to the user and the node can read what the user's processes wrote.
fn uid_gid() -> String {
    let id = |flag: &str| {
        Command::new("id")
            .arg(flag)
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "0".to_owned())
    };
    format!("{}:{}", id("-u"), id("-g"))
}

/// Reads `cpu.stat` (and, on cgroup v1, `cpuacct.usage`) for a container from the places Docker's
/// cgroupfs and systemd drivers put it on cgroup v1 and v2.
fn read_cgroup_cpu(id: &str) -> Option<CpuStat> {
    let parse = |text: &str, key: &str| -> Option<u64> {
        text.lines()
            .find_map(|l| l.strip_prefix(key)?.trim().parse().ok())
    };
    // cgroup v1.
    for dir in [
        format!("/sys/fs/cgroup/cpu,cpuacct/docker/{id}"),
        format!("/sys/fs/cgroup/cpu/docker/{id}"),
        format!("/sys/fs/cgroup/cpu,cpuacct/system.slice/docker-{id}.scope"),
    ] {
        let Ok(stat) = fs::read_to_string(format!("{dir}/cpu.stat")) else {
            continue;
        };
        let usage = fs::read_to_string(format!("{dir}/cpuacct.usage"))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .or_else(|| {
                fs::read_to_string(format!("/sys/fs/cgroup/cpuacct/docker/{id}/cpuacct.usage"))
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
            })
            .unwrap_or(0);
        return Some(CpuStat {
            usage_ns: usage,
            nr_periods: parse(&stat, "nr_periods ").unwrap_or(0),
            nr_throttled: parse(&stat, "nr_throttled ").unwrap_or(0),
            throttled_ns: parse(&stat, "throttled_time ").unwrap_or(0),
        });
    }
    // cgroup v2: microseconds.
    for dir in [
        format!("/sys/fs/cgroup/system.slice/docker-{id}.scope"),
        format!("/sys/fs/cgroup/docker/{id}"),
    ] {
        let Ok(stat) = fs::read_to_string(format!("{dir}/cpu.stat")) else {
            continue;
        };
        return Some(CpuStat {
            usage_ns: parse(&stat, "usage_usec ").unwrap_or(0) * 1000,
            nr_periods: parse(&stat, "nr_periods ").unwrap_or(0),
            nr_throttled: parse(&stat, "nr_throttled ").unwrap_or(0),
            throttled_ns: parse(&stat, "throttled_usec ").unwrap_or(0) * 1000,
        });
    }
    None
}

/// Single-quotes `s` for a POSIX shell.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

impl CapacityBackend for DockerBackend {
    fn name(&self) -> &'static str {
        "docker"
    }

    fn launcher(&self, h: &Handle, real_bin: &Path) -> io::Result<PathBuf> {
        fs::create_dir_all(&self.shim_dir)?;
        let mut limits = Vec::new();
        if let Some(q) = &h.limits.cpu {
            // `--cpu-period`/`--cpu-quota`, not `--cpus`: once NanoCPUs is set Docker refuses to
            // change the period, and a dip changes the quota.
            limits.push(format!("--cpu-period {}", q.period_us));
            limits.push(format!("--cpu-quota {}", q.quota_us));
        }
        if let Some(c) = &h.limits.cpuset {
            limits.push(format!("--cpuset-cpus {}", sh_quote(c)));
        }
        if let Some(m) = h.limits.memory {
            limits.push(format!("--memory {m}"));
        }
        let script = format!(
            r#"#!/usr/bin/env bash
# Generated by sedge: runs one deployed node ({node}) in a container under {lim}.
set -euo pipefail
real={real}
mounts=()
seen=" "
add() {{
  local d=$1 mode=$2
  [[ -n $d && -d $d ]] || return 0
  case $seen in *" $d "*) return 0 ;; esac
  seen="$seen$d "
  mounts+=(-v "$d:$d:$mode")
}}
tmp=${{TMPDIR:-/tmp}}
add "$tmp" rw
add "$(dirname "$real")" ro
IFS=: read -ra libs <<< "${{LD_LIBRARY_PATH:-}}"
for d in "${{libs[@]}}"; do add "$d" ro; done
exec docker run -i --init --name {name} --network host --user {user} \
  {limits} \
  -e LD_LIBRARY_PATH -e TMPDIR="$tmp" -e RUST_BACKTRACE -e RUST_LOG -e RUST_MIN_STACK -e HYDRO_DEPLOY_SEND_BUFFER -e HYDRO_DEPLOY_SEND_STATS \
  "${{mounts[@]}}" {image} "$real" "$@"
"#,
            node = h.node,
            lim = h.limits,
            real = sh_quote(&real_bin.display().to_string()),
            name = sh_quote(&h.name),
            user = uid_gid(),
            limits = limits.join(" "),
            image = sh_quote(&self.image),
        );
        let path = self.shim_dir.join(format!("{}.sh", h.name));
        fs::write(&path, script)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755))?;
        }
        Ok(path)
    }

    fn set_cpu(&self, h: &Handle, quota: CpuQuota) -> Result<(), String> {
        let out = Command::new("docker")
            .args([
                "update",
                "--cpu-quota",
                &quota.quota_us.to_string(),
                &h.name,
            ])
            .output()
            .map_err(|e| format!("docker update: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "docker update {}: {}",
                h.name,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        *h.current_cpu.lock().unwrap() = Some(quota);
        Ok(())
    }

    fn cpu_stat(&self, h: &Handle) -> Option<CpuStat> {
        read_cgroup_cpu(&self.container_id(h)?)
    }

    fn remove(&self, h: &Handle) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &h.name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    fn exit(&self, h: &Handle) -> Option<NodeExit> {
        let out = Command::new("docker")
            .args([
                "inspect",
                "-f",
                "{{.State.Running}} {{.State.OOMKilled}} {{.State.ExitCode}}",
                &h.name,
            ])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let text = String::from_utf8_lossy(&out.stdout);
        let f: Vec<&str> = text.split_whitespace().collect();
        match f.as_slice() {
            ["false", oom, code] => Some(NodeExit {
                oom_killed: *oom == "true",
                exit_code: code.parse().unwrap_or(-1),
            }),
            _ => None,
        }
    }

    fn pids(&self, h: &Handle) -> Vec<u32> {
        let Some(id) = self.container_id(h) else {
            return Vec::new();
        };
        for dir in [
            format!("/sys/fs/cgroup/cpu,cpuacct/docker/{id}"),
            format!("/sys/fs/cgroup/cpu/docker/{id}"),
            format!("/sys/fs/cgroup/cpu,cpuacct/system.slice/docker-{id}.scope"),
            format!("/sys/fs/cgroup/system.slice/docker-{id}.scope"),
            format!("/sys/fs/cgroup/docker/{id}"),
        ] {
            if let Ok(text) = fs::read_to_string(format!("{dir}/cgroup.procs")) {
                return text.lines().filter_map(|l| l.trim().parse().ok()).collect();
            }
        }
        Vec::new()
    }

    fn proc_sampler(&self, name: &str, dir: &Path, cpuset: Option<&str>) -> Option<ProcSampler> {
        fs::create_dir_all(dir).ok()?;
        let _ = fs::remove_file(dir.join("snap"));
        let _ = fs::remove_file(dir.join("pids"));
        let d = dir.display().to_string();
        let mut cmd = Command::new("docker");
        cmd.args(["run", "-d", "--rm", "--init", "--name", name, "--pid", "host"])
            .args(["--network", "none", "--user", &uid_gid()])
            .args(["-v", &format!("{d}:{d}:rw")]);
        if let Some(c) = cpuset {
            cmd.args(["--cpuset-cpus", c]);
        }
        cmd.args([&self.image, "bash", "-c", PROC_SAMPLER_SCRIPT, "sampler", &d]);
        let out = cmd.output().ok()?;
        if !out.status.success() {
            eprintln!(
                "  (proc sampler did not start: {})",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            return None;
        }
        Some(ProcSampler {
            name: name.to_owned(),
            dir: dir.to_owned(),
            last_pids: Mutex::new(String::new()),
        })
    }

    fn remove_prefix(&self, prefix: &str) {
        let Ok(out) = Command::new("docker")
            .args([
                "ps",
                "-aq",
                "--filter",
                &format!("name=^{prefix}"),
            ])
            .output()
        else {
            return;
        };
        let ids: Vec<String> = String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if !ids.is_empty() {
            let _ = Command::new("docker")
                .arg("rm")
                .arg("-f")
                .args(&ids)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }
}

/// The nodes of one trial: every [`Handle`] a [`ContainerHost`] launched, so that the harness can
/// sample, dip and remove them. Dropping it removes every container, so a trial that panics does
/// not leave containers behind.
#[derive(Debug, Clone)]
pub struct Nodes {
    pub backend: Arc<dyn CapacityBackend>,
    handles: Arc<Mutex<Vec<Arc<Handle>>>>,
    guard: Arc<RemoveOnDrop>,
}

#[derive(Debug)]
struct RemoveOnDrop {
    backend: Arc<dyn CapacityBackend>,
    handles: Arc<Mutex<Vec<Arc<Handle>>>>,
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let handles = std::mem::take(&mut *self.handles.lock().unwrap_or_else(|e| e.into_inner()));
        for h in handles {
            self.backend.remove(&h);
        }
    }
}

impl Nodes {
    pub fn new(backend: Arc<dyn CapacityBackend>) -> Self {
        let handles = Arc::new(Mutex::new(Vec::new()));
        Self {
            guard: Arc::new(RemoveOnDrop {
                backend: backend.clone(),
                handles: handles.clone(),
            }),
            backend,
            handles,
        }
    }

    /// A host that launches its binary as node `name` under `limits`.
    pub fn host(
        &self,
        localhost: Arc<LocalhostHost>,
        name: String,
        node: NodeSpec,
        limits: Limits,
    ) -> Arc<ContainerHost> {
        let handle = Arc::new(Handle {
            name,
            node,
            current_cpu: Mutex::new(limits.cpu),
            limits,
            backend_id: OnceLock::new(),
        });
        self.handles.lock().unwrap().push(handle.clone());
        Arc::new(ContainerHost {
            inner: localhost,
            backend: self.backend.clone(),
            handle,
        })
    }

    /// Every node created so far, in creation order.
    pub fn handles(&self) -> Vec<Arc<Handle>> {
        self.handles.lock().unwrap().clone()
    }

    /// Removes every node now (the drop guard then has nothing left to do).
    pub fn remove_all(&self) {
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for h in &handles {
            self.backend.remove(h);
        }
        // Keep the handles for the trace's column names, but they are gone from the backend.
        *self.handles.lock().unwrap() = handles;
        let _ = &self.guard;
    }
}

/// A localhost host whose binary runs under a [`CapacityBackend`]'s limits. Everything but
/// launching is the wrapped [`LocalhostHost`]'s, including its id, so connections between nodes
/// are chosen exactly as between plain localhost processes.
#[derive(Debug)]
pub struct ContainerHost {
    inner: Arc<LocalhostHost>,
    backend: Arc<dyn CapacityBackend>,
    handle: Arc<Handle>,
}

impl ContainerHost {
    pub fn handle(&self) -> &Arc<Handle> {
        &self.handle
    }
}

impl Host for ContainerHost {
    fn target_type(&self) -> HostTargetType {
        self.inner.target_type()
    }

    fn request_port_base(&self, bind_type: &BaseServerStrategy) {
        self.inner.request_port_base(bind_type)
    }

    fn id(&self) -> usize {
        self.inner.id()
    }

    fn request_custom_binary(&self) {
        self.inner.request_custom_binary()
    }

    fn collect_resources(&self, resource_batch: &mut ResourceBatch) {
        self.inner.collect_resources(resource_batch)
    }

    fn provision(&self, resource_result: &Arc<ResourceResult>) -> Arc<dyn LaunchedHost> {
        Arc::new(ContainerLaunchedHost {
            inner: self.inner.provision(resource_result),
            backend: self.backend.clone(),
            handle: self.handle.clone(),
        })
    }

    fn launched(&self) -> Option<Arc<dyn LaunchedHost>> {
        Some(Arc::new(ContainerLaunchedHost {
            inner: self.inner.launched()?,
            backend: self.backend.clone(),
            handle: self.handle.clone(),
        }))
    }

    fn strategy_as_server<'a>(
        &'a self,
        connection_from: &dyn Host,
        network_hint: PortNetworkHint,
    ) -> anyhow::Result<(ClientStrategy<'a>, HostStrategyGetter)> {
        self.inner.strategy_as_server(connection_from, network_hint)
    }

    fn can_connect_to(&self, typ: ClientStrategy<'_>) -> bool {
        self.inner.can_connect_to(typ)
    }
}

struct ContainerLaunchedHost {
    inner: Arc<dyn LaunchedHost>,
    backend: Arc<dyn CapacityBackend>,
    handle: Arc<Handle>,
}

#[async_trait::async_trait]
impl LaunchedHost for ContainerLaunchedHost {
    fn base_server_config(&self, strategy: &BaseServerStrategy) -> ServerBindConfig {
        self.inner.base_server_config(strategy)
    }

    async fn copy_binary(&self, binary: &BuildOutput) -> anyhow::Result<()> {
        self.inner.copy_binary(binary).await
    }

    async fn launch_binary(
        &self,
        id: String,
        binary: &BuildOutput,
        args: &[String],
        tracing: Option<TracingOptions>,
        env: &HashMap<String, String>,
        pin_to_core: Option<usize>,
    ) -> anyhow::Result<Box<dyn LaunchedBinary>> {
        let shim = self.backend.launcher(&self.handle, &binary.bin_path)?;
        // Launching only reads the path and the library directory.
        let shimmed = BuildOutput {
            bin_data: Vec::new(),
            bin_path: shim,
            shared_library_paths: binary.shared_library_paths.clone(),
        };
        self.inner
            .launch_binary(id, &shimmed, args, tracing, env, pin_to_core)
            .await
    }

    async fn forward_port(&self, addr: &SocketAddr) -> anyhow::Result<SocketAddr> {
        self.inner.forward_port(addr).await
    }
}

/// A fingerprint of the host for the calibration cache: host name, core count and CPU model.
pub fn host_fingerprint() -> String {
    let hostname = fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_owned())
        .unwrap_or_else(|_| "unknown".to_owned());
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = cpuinfo
        .lines()
        .find_map(|l| l.strip_prefix("model name"))
        .map(|l| l.trim_start_matches([' ', '\t', ':']).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    // The machine's cores, not this process's affinity (the harness pins itself).
    let cores = cpuinfo
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count();
    format!("{hostname}/{cores}c/{model}")
}
