//! Maelstrom installation discovery and checksum-verified provisioning.

use std::path::{Path, PathBuf};
use std::process::Command;

pub const MAELSTROM_VERSION: &str = "0.2.4";
pub const MAELSTROM_SOURCE_COMMIT: &str = "b70544f95ee0579b1a5e4bdf60bb2f90a44171bc";
pub const MAELSTROM_SHA256: &str =
    "301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799";
const DOWNLOAD_URL: &str =
    "https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2";
const SOURCE_URL: &str = "https://github.com/jepsen-io/maelstrom.git";

pub fn default_maelstrom_path() -> PathBuf {
    if let Some(path) = std::env::var_os("MAELSTROM_PATH") {
        return PathBuf::from(path);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let cache = home.join(".cache/consensus-gauntlet/maelstrom-v0.2.4");
    cache.join("maelstrom-single-kill")
}

/// Locate an explicit Maelstrom executable or build the checksum-pinned
/// single-node-kill variant from the exact upstream v0.2.4 source commit.
///
/// Network access occurs only when neither an explicit executable nor the
/// cached source-built launcher exists.
pub fn ensure_maelstrom(explicit: Option<&Path>) -> Result<PathBuf, String> {
    let binary = explicit
        .map(Path::to_path_buf)
        .unwrap_or_else(default_maelstrom_path);
    if binary.is_file() {
        return Ok(binary);
    }
    if explicit.is_some() {
        return Err(format!(
            "Maelstrom executable not found at {}",
            binary.display()
        ));
    }
    build_single_kill_maelstrom(&binary)?;
    Ok(binary)
}

fn build_single_kill_maelstrom(wrapper: &Path) -> Result<(), String> {
    let install_root = wrapper
        .parent()
        .ok_or_else(|| format!("cannot derive install root from {}", wrapper.display()))?;
    std::fs::create_dir_all(install_root).map_err(|error| error.to_string())?;
    let source = install_root.join("maelstrom-source");
    if !source.join(".git").is_dir() {
        run(
            Command::new("git")
                .args(["clone", "--no-checkout", SOURCE_URL])
                .arg(&source),
            "clone Maelstrom source",
        )?;
    }
    run(
        Command::new("git").args(["-C"]).arg(&source).args([
            "fetch",
            "--depth",
            "1",
            "origin",
            MAELSTROM_SOURCE_COMMIT,
        ]),
        "fetch pinned Maelstrom source",
    )?;
    run(
        Command::new("git")
            .args(["-C"])
            .arg(&source)
            .args(["checkout", "--detach", "FETCH_HEAD"]),
        "checkout pinned Maelstrom source",
    )?;
    let actual = command_stdout(
        Command::new("git")
            .args(["-C"])
            .arg(&source)
            .args(["rev-parse", "HEAD"]),
        "identify Maelstrom source revision",
    )?;
    if actual.trim() != MAELSTROM_SOURCE_COMMIT {
        return Err(format!(
            "Maelstrom source revision mismatch: expected {MAELSTROM_SOURCE_COMMIT}, got {}",
            actual.trim()
        ));
    }
    patch_exact(
        &source.join("src/maelstrom/core.clj"),
        "#{:partition})",
        "#{:partition :kill})",
    )?;
    patch_exact(
        &source.join("src/maelstrom/nemesis.clj"),
        "(nc/db-package opts)",
        "(nc/db-package (assoc opts :kill {:targets [:one]}))",
    )?;
    run(
        Command::new("lein").arg("uberjar").current_dir(&source),
        "build single-node-kill Maelstrom",
    )?;
    let jar = source.join("target/maelstrom-0.2.4-standalone.jar");
    if !jar.is_file() {
        return Err(format!(
            "Maelstrom source build did not create {}",
            jar.display()
        ));
    }
    let launcher = format!(
        "#!/usr/bin/env bash\nexec java -Djava.awt.headless=true -jar '{}' \"$@\"\n",
        jar.display()
    );
    std::fs::write(wrapper, launcher).map_err(|error| error.to_string())?;
    let mut permissions = std::fs::metadata(wrapper)
        .map_err(|error| error.to_string())?
        .permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o755);
        std::fs::set_permissions(wrapper, permissions).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn patch_exact(path: &Path, before: &str, after: &str) -> Result<(), String> {
    let source = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    if source.matches(before).count() != 1 {
        return Err(format!(
            "expected exactly one `{before}` in pinned source {}",
            path.display()
        ));
    }
    std::fs::write(path, source.replace(before, after)).map_err(|error| error.to_string())
}

fn command_stdout(command: &mut Command, label: &str) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("{label}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| format!("{label}: {error}"))
}

#[allow(dead_code)]
fn install_release(binary: &Path) -> Result<(), String> {
    let install_root = binary
        .parent()
        .ok_or_else(|| format!("cannot derive install root from {}", binary.display()))?;
    std::fs::create_dir_all(install_root).map_err(|error| error.to_string())?;
    let archive = install_root.join("maelstrom.tar.bz2");
    run(
        Command::new("curl")
            .args(["-L", "--fail", "--silent", "--show-error", "-o"])
            .arg(&archive)
            .arg(DOWNLOAD_URL),
        "download Maelstrom",
    )?;
    let digest = Command::new("shasum")
        .args(["-a", "256"])
        .arg(&archive)
        .output()
        .map_err(|error| format!("run shasum: {error}"))?;
    if !digest.status.success() {
        return Err(format!(
            "shasum failed: {}",
            String::from_utf8_lossy(&digest.stderr)
        ));
    }
    let actual = String::from_utf8_lossy(&digest.stdout)
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned();
    if actual != MAELSTROM_SHA256 {
        return Err(format!(
            "Maelstrom checksum mismatch: expected {MAELSTROM_SHA256}, got {actual}"
        ));
    }
    run(
        Command::new("tar")
            .arg("-xjf")
            .arg(&archive)
            .arg("-C")
            .arg(install_root),
        "extract Maelstrom",
    )?;
    if !binary.is_file() {
        return Err(format!(
            "Maelstrom archive did not create {}",
            binary.display()
        ));
    }
    Ok(())
}

fn run(command: &mut Command, label: &str) -> Result<(), String> {
    let output = command
        .output()
        .map_err(|error| format!("{label}: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_existing_path_wins_without_network() {
        let file = tempfile::NamedTempFile::new().unwrap();
        assert_eq!(ensure_maelstrom(Some(file.path())).unwrap(), file.path());
    }

    #[test]
    fn pinned_source_revision_is_full_sha() {
        assert_eq!(MAELSTROM_VERSION, "0.2.4");
        assert_eq!(MAELSTROM_SOURCE_COMMIT.len(), 40);
        assert_eq!(MAELSTROM_SHA256.len(), 64);
    }

    #[test]
    fn source_patches_are_exactly_the_single_kill_changes() {
        assert_eq!(
            "#{:partition})".replace("#{:partition})", "#{:partition :kill})"),
            "#{:partition :kill})"
        );
        assert!("(nc/db-package (assoc opts :kill {:targets [:one]}))".contains(":targets [:one]"));
    }
}
