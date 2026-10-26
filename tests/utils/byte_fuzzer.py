"""
Byte-level mutational fuzzing of serialized messages.

Ported from jelly-jvm's `ByteFuzzer`. Valid messages are edited at random. Some
of them still parse as protobuf, but their contents are invalid, and the reader
must reject them with its own error.

Runs are seeded, so a finding can be reproduced from the seed and iteration. Set
the JELLY_FUZZ_ITERATIONS and JELLY_FUZZ_SEED environment variables to change
them, e.g. for a longer run.
"""

from __future__ import annotations

import os
import random
import traceback
from collections.abc import Callable, Sequence
from typing import Final

ITERATIONS: Final = int(os.environ.get("JELLY_FUZZ_ITERATIONS", "30000"))
SEED: Final = int(os.environ.get("JELLY_FUZZ_SEED", "20260918"))

MAX_REPORTED: Final = 5
MAX_SIZE: Final = 1 << 16

# Bytes with a special meaning in protobuf: the smallest and largest one-byte
# varints, a continuation bit, and the tag of field 1
INTERESTING: Final = (0x00, 0x01, 0x7F, 0x80, 0xFF, 0x0A)


def mutate(data: bytes, rnd: random.Random) -> bytes:
    """Apply 1-4 random edits to the bytes."""
    for _ in range(rnd.randrange(1, 5)):
        edited = _edit(data, rnd)
        if len(edited) <= MAX_SIZE:
            data = edited
    return data


def _edit(data: bytes, rnd: random.Random) -> bytes:  # noqa: PLR0911
    if not data:
        return data
    size = len(data)
    at = rnd.randrange(size)
    edit = rnd.randrange(6)
    if edit == 0:
        # Flip one bit
        return data[:at] + bytes([data[at] ^ (1 << rnd.randrange(8))]) + data[at + 1 :]
    if edit == 1:
        # Replace one byte with a random value
        return data[:at] + bytes([rnd.randrange(256)]) + data[at + 1 :]
    if edit == 2:  # noqa: PLR2004
        # Replace one byte with one of INTERESTING
        return data[:at] + bytes([rnd.choice(INTERESTING)]) + data[at + 1 :]
    if edit == 3:  # noqa: PLR2004
        # Cut the message short
        return data[:at]
    if edit == 4:  # noqa: PLR2004
        # Repeat a slice, so repeated fields and submessages get longer
        end = at + rnd.randrange(size - at) + 1
        return data[:end] + data[at:end] + data[end:]
    # Insert 1-4 random bytes
    return data[:at] + rnd.randbytes(rnd.randrange(1, 5)) + data[at:]


def findings(
    corpus: Sequence[bytes],
    consume: Callable[[bytes], object],
    expected: tuple[type[BaseException], ...],
    iterations: int = ITERATIONS,
    seed: int = SEED,
) -> list[str]:
    """
    Feed mutated copies of the corpus to `consume`.

    Returns:
        list[str]: one description per exception that is not `expected`, with
            enough to reproduce it.

    """
    rnd = random.Random(seed)  # noqa: S311
    found: list[str] = []
    count = 0
    for i in range(iterations):
        data = mutate(rnd.choice(corpus), rnd)
        try:
            consume(data)
        except expected:
            pass
        except Exception as error:  # noqa: BLE001
            count += 1
            if len(found) < MAX_REPORTED:
                found.append(_describe(seed, i, data, error))
    if count > len(found):
        found.append(f"...and {count - len(found)} more")
    return found


def _describe(seed: int, iteration: int, data: bytes, error: Exception) -> str:
    where = traceback.extract_tb(error.__traceback__)[-1]
    more = f"... ({len(data)} bytes)" if len(data) > 96 else ""  # noqa: PLR2004
    return (
        f"seed {seed}, iteration {iteration}: {type(error).__name__}: {error}\n"
        f"    at {where.filename}:{where.lineno}\n"
        f"    input: {data[:96].hex()}{more}"
    )
