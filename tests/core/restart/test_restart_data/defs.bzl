# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _impl_cp(ctx):
    out = ctx.actions.declare_output("out", has_content_based_path = False)
    ctx.actions.run(
        cmd_args(["cp", ctx.attrs.src, out.as_output()], hidden = ctx.attrs.dep or []),
        category = "cp",
        local_only = ctx.attrs.local_only,
        # An action that reads another target's output has to run where that output is not already
        # on disk, or the build never asks the CAS for it and an eviction there goes unnoticed.
        prefer_remote = not ctx.attrs.local_only,
        env = {"CACHE_BUSTER": str(ctx.attrs.local_only)},
    )
    return [DefaultInfo(out)]

cp = rule(
    attrs = {
        # A dependency the copy does not read, so the output digest comes from `src` alone. A chain
        # built this way has a distinct digest at every level, so a test can make one level's blob
        # disappear without touching any other level's.
        "dep": attrs.option(attrs.source(), default = None),
        "local_only": attrs.bool(default = False),
        "src": attrs.source(),
    },
    impl = _impl_cp,
)
