//! `stormlb-test`: the container's entrypoint. See the library for what the
//! suites do; this reads the runner's environment, runs one suite within
//! `STORM_TIMEOUT`, and exits 0, 1 or 2.

use stormlb_test::env::Env;
use stormlb_test::report::{Outcome, Report};

fn main() {
    let env = Env::read();
    let mut r = Report::new();
    let missing = env.missing();
    if !missing.is_empty() {
        r.record("environment", Outcome::Infra(format!("the runner did not set {}", missing.join(", "))), 0, None);
        std::process::exit(r.finish());
    }
    let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            r.record("runtime", Outcome::Infra(format!("tokio runtime: {e}")), 0, None);
            std::process::exit(r.finish());
        }
    };
    let deadline = env.timeout;
    let ran = rt.block_on(async { tokio::time::timeout(deadline, stormlb_test::run(&env, &mut r)).await });
    if ran.is_err() {
        r.record(
            "timeout",
            Outcome::Fail(format!("the suite did not finish within STORM_TIMEOUT ({} s)", deadline.as_secs())),
            deadline.as_millis(),
            None,
        );
    }
    let code = r.finish();
    // Backend listeners and in-flight tasks are the runtime's; do not wait
    // for them.
    rt.shutdown_background();
    std::process::exit(code);
}
