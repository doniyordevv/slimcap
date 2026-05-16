#!/usr/bin/env python3
"""Validate a slimcap transcode: input.mcap vs output.mcap.

Checks, per topic:
  * passthrough topics (output schema unchanged): byte-for-byte identical
    message set — same (sequence, log_time, publish_time, sha256(data)).
    This is the hard guarantee for IMU / SLAM data.
  * video topics (output schema foxglove.CompressedVideo): 1:1 frame count
    preserved vs the input image topic.
  * every output topic: log_time is monotonically non-decreasing.

Usage:  python3 tools/validate.py INPUT.mcap OUTPUT.mcap
Exit code 0 = all checks passed.
"""
import hashlib
import sys
from collections import defaultdict

from mcap.reader import make_reader

VIDEO_SCHEMA = "foxglove.CompressedVideo"


def scan(path):
    """topic -> (schema_name, list[(seq, log_time, pub_time, sha256)])
    plus topic -> list[log_time] in file order."""
    msgs = defaultdict(list)
    schema_of = {}
    order = defaultdict(list)
    with open(path, "rb") as f:
        reader = make_reader(f)
        for schema, channel, message in reader.iter_messages():
            t = channel.topic
            schema_of[t] = schema.name if schema else ""
            msgs[t].append(
                (
                    message.sequence,
                    message.log_time,
                    message.publish_time,
                    hashlib.sha256(message.data).digest(),
                )
            )
            order[t].append(message.log_time)
    return schema_of, msgs, order


def main():
    inp, out = sys.argv[1], sys.argv[2]
    print(f"input : {inp}")
    print(f"output: {out}\n")
    si, mi, _ = scan(inp)
    so, mo, oo = scan(out)

    ok = True
    topics = sorted(set(si) | set(so))
    width = max(len(t) for t in topics)
    for t in topics:
        in_n, out_n = len(mi.get(t, [])), len(mo.get(t, []))
        is_video = so.get(t) == VIDEO_SCHEMA

        # monotonic log_time on output
        seq = oo.get(t, [])
        mono = all(seq[i] <= seq[i + 1] for i in range(len(seq) - 1))

        if is_video:
            status = "OK" if in_n == out_n and mono else "FAIL"
            detail = f"video  in={in_n} out={out_n} frames "
            detail += "monotonic" if mono else "NON-MONOTONIC"
        else:
            same = sorted(mi.get(t, [])) == sorted(mo.get(t, []))
            status = "OK" if same and mono else "FAIL"
            detail = "passthrough byte-identical" if same else (
                f"passthrough DIFF (in={in_n} out={out_n})"
            )
            if not mono:
                detail += " + NON-MONOTONIC"
        if status != "OK":
            ok = False
        print(f"  [{status:4}] {t:<{width}}  {detail}  [{si.get(t,'-')} -> {so.get(t,'-')}]")

    print("\n" + ("ALL CHECKS PASSED" if ok else "VALIDATION FAILED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
