# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict


import asyncio
import json
import os
import signal
from pathlib import Path

from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.asserts import expect_failure
from buck2.tests.e2e_util.buck_workspace import buck_test
from buck2.tests.e2e_util.helper.utils import read_invocation_record

TEST_DIGEST = "76f7aea8c1fc400287312b9608ceb24848ba02ac:14"


@buck_test()
async def test_restart_requires_no_stdout(buck: Buck) -> None:
    res = await buck.targets("//:stage0", env={"FORCE_WANT_RESTART": "true"})
    assert res.stdout.count("//:stage0") == 1


@buck_test()
async def test_restart(buck: Buck) -> None:
    # Normally shows once.
    res = await expect_failure(buck.targets("//:invalid"))
    assert res.stderr.count("Unknown target `invalid`") == 1

    # But if we force a restart...
    res = await expect_failure(
        buck.targets("//:invalid", env={"FORCE_WANT_RESTART": "true"})
    )
    assert res.stderr.count("Unknown target `invalid`") == 2


@buck_test(allow_soft_errors=True)
async def test_restart_materializer_corruption(buck: Buck) -> None:
    stage1 = "//:stage1"
    res = await buck.build(stage1)
    out = res.get_build_report().output_for_target(stage1)

    # Now we remove this file (which comes to us via RE)
    # Only way to get it back is by killing the materializer state.
    os.unlink(out)

    res = await buck.build("//:stage2")
    assert "Your command will now restart" in res.stderr


@buck_test(allow_soft_errors=True)
async def test_restart_cas_missing(buck: Buck) -> None:
    # Make sure Buck is not running.
    await buck.kill()

    # Start a daemon with the `src` file tombstoned. This means we cannot download it from RE.
    # This is just the hash of `src`.
    await buck.build(env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST})

    # Now build //:stage2. Buck2 must try to download the file, fail, then
    # restart the daemon.
    res = await buck.build("//:stage2")
    assert "Your command will now restart" in res.stderr

    # TODO: We should also handle the case where the top-level artifact is what
    # fails to download under the default materialization setting.
    # `test_restart_cas_missing_top_level_artifact` covers it only with
    # `--materializations=all`.


@buck_test(allow_soft_errors=True)
async def test_restart_cas_missing_top_level_artifact(buck: Buck) -> None:
    # Make sure Buck is not running.
    await buck.kill()

    await buck.build(env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST})

    # //:stage1 is itself the artifact that fails to download: materializing a requested
    # top-level output has no producing-action execution attempt for CAS-missing recovery to
    # attribute a repair to, so this stays on the existing daemon-rejecting restart path.
    #
    # `--materializations=all` requests that download unconditionally. Leaving the setting at
    # its default routes the download through `try_materialize_final_artifact`, which reports
    # a missing digest without the tag the restarter reads, so the build fails outright.
    res = await buck.build("//:stage1", "--materializations=all")
    assert "Your command will now restart" in res.stderr


@buck_test(
    allow_soft_errors=True,
    skip_for_os=["windows"],
)
async def test_restart_forkserver_crash(buck: Buck) -> None:
    # Start the daemon
    await buck.build()

    # Kill its forkserver.
    forkserver_pid = json.loads((await buck.status()).stdout)["forkserver_pid"]
    assert forkserver_pid is not None
    os.kill(forkserver_pid, signal.SIGKILL)

    # Wait for its forkserver to exit.
    for _ in range(10):
        try:
            os.kill(forkserver_pid, 0)
        except OSError:
            break
        else:
            await asyncio.sleep(1)

    # Now build a thing and check we restart
    res = await buck.build("//:stage2")
    assert "Your command will now restart" in res.stderr


@buck_test()
async def test_restart_disabled(buck: Buck) -> None:
    # Ensure no daemon
    await buck.kill()

    with open(buck.cwd / ".buckconfig", "a") as f:
        f.write("[buck2]\nrestarter = false")

    result = await expect_failure(
        buck.build(
            "//:stage2",
            env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST},
        ),
    )
    assert "Your command will now restart" not in result.stderr


@buck_test(write_invocation_record=True)
async def test_trace_id(buck: Buck) -> None:
    trace_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    # But if we force a restart...
    res = await expect_failure(
        buck.targets(
            "//:invalid",
            env={"FORCE_WANT_RESTART": "true", "BUCK_WRAPPER_UUID": trace_id},
        )
    )
    record = res.invocation_record()
    assert record["trace_id"] != trace_id
    assert record["restarted_trace_id"] == trace_id
    assert record["should_restart"] is False


async def _daemon_pid(buck: Buck) -> int:
    status = json.loads((await buck.status()).stdout)
    return status["process_info"]["pid"]


def _enable_cas_missing_recovery(buck: Buck, max_command_retries: int = 1) -> None:
    with open(buck.cwd / ".buckconfig", "a") as f:
        f.write(
            "[buck2]\n"
            "cas_missing_recovery = true\n"
            f"cas_missing_recovery_max_command_retries = {max_command_retries}\n"
        )


@buck_test(allow_soft_errors=True)
async def test_cas_missing_recovery_repairs_without_a_new_daemon(buck: Buck) -> None:
    # Make sure Buck is not running.
    await buck.kill()

    _enable_cas_missing_recovery(buck)

    # //:stage1 and //:stage2 both copy `src` unchanged, so :stage1's output shares `src`'s
    # digest. Tombstoning that digest fails :stage2's attempt to materialize :stage1's output
    # (it is `local_only`), and CAS-missing recovery attributes that failure back to :stage1's
    # action and arms it for repair. The client's own automatic retry (also gated by
    # cas_missing_recovery) may fire here too, but the tombstone is still active, so it fails
    # the same way; this build's outer result is still a failure either way.
    res = await expect_failure(
        buck.build("//:stage2", env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST})
    )
    assert "queued them for re-execution" in res.stderr

    daemon_pid = await _daemon_pid(buck)

    # The digest is available again — in a real failure this is the RE CAS's own TTL catching
    # up; here the test clears the fault injection directly, simulating that without touching
    # the daemon that queued the repair for :stage1's action.
    await buck.audit("deferred-materializer", "clear-tombstoned-test-digests")

    res = await buck.build("//:stage2")
    assert res.get_build_report().output_for_target("//:stage2").exists()

    # The property this test exists to prove: the repair happened without ever discarding the
    # daemon that queued it.
    assert await _daemon_pid(buck) == daemon_pid


@buck_test(allow_soft_errors=True)
async def test_cas_missing_recovery_retries_automatically_within_one_invocation(
    buck: Buck, tmp_path: Path
) -> None:
    await buck.kill()

    # A generous retry budget gives the concurrent clear below several chances to land between
    # two attempts, without depending on exactly which attempt it beats.
    _enable_cas_missing_recovery(buck, max_command_retries=5)

    await buck.build(env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST})
    daemon_pid = await _daemon_pid(buck)

    record_file = tmp_path / "record.json"
    build = buck.build(
        "//:stage2",
        "--unstable-write-invocation-record",
        str(record_file),
    )

    async def clear_tombstone_mid_build() -> None:
        # The retry runs in-process against the already-running daemon, so it follows the
        # first failure almost immediately; this delay only needs to clear the tombstone after
        # the first attempt has had time to fail, not before.
        await asyncio.sleep(3)
        await buck.audit("deferred-materializer", "clear-tombstoned-test-digests")

    _, res = await asyncio.gather(clear_tombstone_mid_build(), build)

    assert res.get_build_report().output_for_target("//:stage2").exists()

    record = read_invocation_record(record_file)
    assert record["restarted_trace_id"] is not None

    assert await _daemon_pid(buck) == daemon_pid


@buck_test(allow_soft_errors=True)
async def test_cas_missing_recovery_command_retry_is_bounded(buck: Buck) -> None:
    await buck.kill()

    _enable_cas_missing_recovery(buck, max_command_retries=1)

    # The digest stays tombstoned for the whole test: every repair attempt reproduces the
    # identical digest, so every attempt fails the same way, and the budget of one command
    # retry must stop the client from retrying forever.
    res = await expect_failure(
        buck.build("//:stage2", env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST})
    )
    assert res.stderr.count("Your command will now restart") == 1


@buck_test()
async def test_cas_missing_recovery_disabled_does_not_retry(buck: Buck) -> None:
    # cas_missing_recovery defaults to disabled, so this is today's behavior: no automatic
    # command retry, and a materialization failure stays on the existing daemon-rejecting
    # restart path, which test_restart_disabled already covers with restarter turned off.
    await buck.kill()

    result = await expect_failure(
        buck.build(
            "//:stage2",
            env={"BUCK2_TEST_TOMBSTONED_DIGESTS": TEST_DIGEST},
        ),
    )
    assert "queued them for re-execution" not in result.stderr
