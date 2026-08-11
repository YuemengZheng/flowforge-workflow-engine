"""Variable pool: namespaced node outputs and ``{{node.field}}`` resolution.

One pool per run, so two concurrent runs of the same graph never see each
other's values. Within a run, writes only happen in the scheduler's settle
phase (between waves) and reads only happen while nodes execute, so no
locking is needed on the single-threaded event loop.

Resolution is split into a **compile** step and a **render** step, because the
two have very different lifetimes. A node's config is a fixed piece of JSON: the
regex scan that finds its references, the ``a.b.0.c`` splitting, and the parsing
of a ``?? fallback`` literal all depend on the config alone and never on the
pool, yet a naive resolver redoes them on every attempt of every node of every
run. :func:`compile_template` does that work once and returns a plan; the plan's
``render`` walks straight to dict lookups.

Two consequences worth knowing:

* A subtree with no references in it compiles to a static plan that renders as
  *the original object*, so a config without templates costs one dict lookup
  instead of a full copy. Resolved config is therefore **read-only** — nodes read
  ``ctx.config``, they must not mutate it, or they would be writing into the
  graph's own spec.
* :meth:`VariablePool.resolve_uncached` is kept as the reference implementation.
  It is what the benchmark's baseline arm measures and what the equivalence test
  checks the compiler against, which is worth more than the few lines it costs.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any, Iterator, Mapping, Sequence

from .errors import FlowForgeError

# {{ node.field.sub }}  with an optional  ?? fallback  literal
_REFERENCE = re.compile(r"\{\{\s*(?P<body>[^{}]+?)\s*\}\}")
_DEFAULT_SEP = "??"


class VariableError(FlowForgeError):
    """A template referenced something the pool does not hold."""


#: Distinguishes "key absent" from "key present holding None", without the
#: second dict lookup that ``in`` then ``[]`` would cost.
_MISSING = object()


def _stringify(value: Any) -> str:
    """Render a value for interpolation into surrounding text."""
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, ensure_ascii=False, default=str)


class VariablePool:
    """Namespace -> outputs, plus template resolution over that namespace.

    Namespaces are node ids, with ``inputs`` reserved for the run's own inputs::

        {{ inputs.question }}      the run input
        {{ fetch_user.name }}      an upstream node's output field
        {{ rows.0.id }}            index into a list
        {{ maybe.value ?? "n/a" }} fallback when the reference is missing
    """

    RUN_NAMESPACE = "inputs"

    def __init__(self, inputs: Mapping[str, Any] | None = None) -> None:
        self._values: dict[str, Mapping[str, Any]] = {}
        if inputs is not None:
            self._values[self.RUN_NAMESPACE] = dict(inputs)

    # ------------------------------------------------------------------ write

    def set_outputs(self, namespace: str, outputs: Mapping[str, Any]) -> None:
        self._values[namespace] = dict(outputs)

    def __contains__(self, namespace: object) -> bool:
        return namespace in self._values

    def namespaces(self) -> list[str]:
        return sorted(self._values)

    def snapshot(self) -> dict[str, Any]:
        return {ns: dict(values) for ns, values in self._values.items()}

    # ------------------------------------------------------------------- read

    def get(self, path: str) -> Any:
        """Look up ``a.b.0.c``. Raises :class:`VariableError` if absent."""
        return self.lookup(tuple(seg for seg in path.split(".") if seg), path)

    def lookup(self, segments: Sequence[str], path: str) -> Any:
        """Same as :meth:`get` with the path already split.

        Compiled templates carry the split form, so the hot path does no string
        work at all. ``path`` is passed through only to keep error messages
        pointing at what the author actually wrote.
        """
        if not segments:
            raise VariableError("empty variable reference")
        namespace = segments[0]
        if namespace not in self._values:
            raise VariableError(
                f"unknown variable namespace {namespace!r} in {path!r}; "
                f"available: {', '.join(self.namespaces()) or '<none>'}"
            )
        cursor: Any = self._values[namespace]
        # The plain-dict hit is the case that actually happens, so it is checked
        # first and without building anything: the error context (``walked``) is
        # assembled only on the path that is about to raise.
        for index in range(1, len(segments)):
            segment = segments[index]
            if cursor.__class__ is dict:
                found = cursor.get(segment, _MISSING)
                if found is not _MISSING:
                    cursor = found
                    continue
            cursor = self._step(cursor, segment, path, ".".join(segments[:index]))
        return cursor

    @staticmethod
    def _step(cursor: Any, segment: str, path: str, walked: str) -> Any:
        if isinstance(cursor, Mapping):
            if segment in cursor:
                return cursor[segment]
            raise VariableError(f"{walked!r} has no field {segment!r} (in {path!r})")
        if isinstance(cursor, Sequence) and not isinstance(cursor, (str, bytes)):
            if not segment.lstrip("-").isdigit():
                raise VariableError(
                    f"{walked!r} is a list, {segment!r} is not an index (in {path!r})"
                )
            index = int(segment)
            try:
                return cursor[index]
            except IndexError:
                raise VariableError(
                    f"{walked!r} has {len(cursor)} items, index {index} is out of range"
                ) from None
        raise VariableError(
            f"{walked!r} is a {type(cursor).__name__}, cannot read {segment!r} from it"
        )

    # --------------------------------------------------------------- resolve

    def resolve(self, value: Any) -> Any:
        """Substitute references in strings, dict values and lists.

        A string that is exactly one reference keeps the referenced value's
        type (``"{{a.rows}}"`` yields the list, not its ``str()``); a reference
        embedded in surrounding text is stringified and interpolated.

        Compiles ``value`` and renders it. Callers that resolve the same value
        repeatedly should hold the plan themselves — see :func:`compile_template`
        — since only the string-level compilation is memoised here.
        """
        return compile_template(value).render(self)

    def resolve_uncached(self, value: Any) -> Any:
        """The reference implementation of :meth:`resolve`: parse as you walk.

        Kept deliberately. The benchmark needs a baseline arm, and
        ``tests/test_template.py`` needs an oracle to check the compiler against
        on inputs neither of us thought to special-case.
        """
        if isinstance(value, str):
            return self._resolve_string(value)
        if isinstance(value, Mapping):
            return {key: self.resolve_uncached(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            resolved = [self.resolve_uncached(item) for item in value]
            return type(value)(resolved) if isinstance(value, tuple) else resolved
        return value

    def _resolve_string(self, text: str) -> Any:
        whole = _REFERENCE.fullmatch(text.strip())
        if whole is not None:
            return self._lookup(whole.group("body"))
        return _REFERENCE.sub(lambda m: self._stringify(self._lookup(m.group("body"))), text)

    def _lookup(self, body: str) -> Any:
        path, _, fallback = body.partition(_DEFAULT_SEP)
        try:
            return self.get(path.strip())
        except VariableError:
            if not fallback.strip():
                raise
            return _literal(fallback.strip())

    _stringify = staticmethod(_stringify)

    def references(self, value: Any) -> Iterator[str]:
        """Every reference path appearing in ``value`` — used for validation."""
        if isinstance(value, str):
            for match in _REFERENCE.finditer(value):
                yield match.group("body").partition(_DEFAULT_SEP)[0].strip()
        elif isinstance(value, Mapping):
            for item in value.values():
                yield from self.references(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from self.references(item)

    def __repr__(self) -> str:
        return f"<VariablePool namespaces={self.namespaces()}>"


def _literal(text: str) -> Any:
    """Parse a fallback literal: JSON if possible, otherwise the bare string."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


# --------------------------------------------------------------- compilation
#
# Four plan shapes, each with a ``static`` flag and a ``render``. ``static``
# propagates upward: a container whose children are all static is itself static,
# which is what collapses a template-free config into a single object.


class Template:
    """A compiled value. ``render`` is the only thing the hot path calls."""

    __slots__ = ()
    static: bool = False

    def render(self, pool: VariablePool) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError


class _Static(Template):
    """No references anywhere below here: hand back the original object."""

    __slots__ = ("value",)
    static = True

    def __init__(self, value: Any) -> None:
        self.value = value

    def render(self, pool: VariablePool) -> Any:
        return self.value


class _Reference(Template):
    """A string that is exactly one reference, so the value keeps its type."""

    __slots__ = ("path", "segments", "fallback", "has_fallback")

    def __init__(self, body: str) -> None:
        path, separator, fallback = body.partition(_DEFAULT_SEP)
        self.path = path.strip()
        self.segments = tuple(seg for seg in self.path.split(".") if seg)
        self.has_fallback = bool(separator) and bool(fallback.strip())
        # Parsed at compile time: the fallback is a literal, not a reference.
        self.fallback = _literal(fallback.strip()) if self.has_fallback else None

    def render(self, pool: VariablePool) -> Any:
        # `{{node.field}}` is the overwhelmingly common shape, and it is two dict
        # lookups. Anything else — deeper paths, a missing namespace, a list index,
        # a fallback to apply — goes the general way, which is also where the
        # error messages are built.
        if len(self.segments) == 2:
            namespace = pool._values.get(self.segments[0])
            if namespace.__class__ is dict:
                found = namespace.get(self.segments[1], _MISSING)
                if found is not _MISSING:
                    return found
        try:
            return pool.lookup(self.segments, self.path)
        except VariableError:
            if not self.has_fallback:
                raise
            return self.fallback


class _Interpolation(Template):
    """References embedded in surrounding text: stringify and join."""

    __slots__ = ("parts",)

    def __init__(self, parts: tuple[Any, ...]) -> None:
        self.parts = parts

    def render(self, pool: VariablePool) -> str:
        return "".join(
            part if part.__class__ is str else _stringify(part.render(pool))
            for part in self.parts
        )


class _MappingTemplate(Template):
    """A dict with at least one reference somewhere inside it.

    Only the keys that actually contain references are re-rendered. The static
    ones are pre-placed in ``base`` in their original positions, so ``base.copy()``
    (a C-level operation) does the bulk of the work and overwriting a key in place
    preserves insertion order — a resolved config keeps the shape it was authored
    in.
    """

    __slots__ = ("base", "dynamic")

    def __init__(self, items: tuple[tuple[Any, Template], ...]) -> None:
        self.base = {key: (plan.value if plan.static else None) for key, plan in items}
        self.dynamic = tuple((key, plan) for key, plan in items if not plan.static)

    def render(self, pool: VariablePool) -> dict[Any, Any]:
        rendered = self.base.copy()
        for key, plan in self.dynamic:
            rendered[key] = plan.render(pool)
        return rendered


class _SequenceTemplate(Template):
    __slots__ = ("base", "dynamic", "as_tuple")

    def __init__(self, plans: tuple[Template, ...], as_tuple: bool) -> None:
        self.base = [plan.value if plan.static else None for plan in plans]
        self.dynamic = tuple(
            (index, plan) for index, plan in enumerate(plans) if not plan.static
        )
        self.as_tuple = as_tuple

    def render(self, pool: VariablePool) -> Any:
        rendered = self.base.copy()
        for index, plan in self.dynamic:
            rendered[index] = plan.render(pool)
        return tuple(rendered) if self.as_tuple else rendered


def compile_template(value: Any) -> Template:
    """Compile a config value into a plan that can be rendered many times.

    Cheap to call repeatedly on the same *strings* — those are memoised — but a
    container is walked each time, so a caller resolving one config over and over
    (the engine, once per node) should keep the plan it gets back.
    """
    if isinstance(value, str):
        return compile_string(value)
    if isinstance(value, Mapping):
        items = tuple((key, compile_template(item)) for key, item in value.items())
        if all(plan.static for _, plan in items):
            return _Static(value)
        return _MappingTemplate(items)
    if isinstance(value, (list, tuple)):
        plans = tuple(compile_template(item) for item in value)
        if all(plan.static for plan in plans):
            return _Static(value)
        return _SequenceTemplate(plans, isinstance(value, tuple))
    return _Static(value)


@lru_cache(maxsize=8192)
def compile_string(text: str) -> Template:
    """Compile one string. Memoised: config strings repeat across nodes and runs.

    Bounded rather than unbounded — template text is authored, so the working set
    is small, but a caller generating templates at runtime should not be able to
    grow this without limit.
    """
    if "{{" not in text:
        # The overwhelmingly common case, and the cheapest possible check.
        return _Static(text)

    whole = _REFERENCE.fullmatch(text.strip())
    if whole is not None:
        return _Reference(whole.group("body"))

    parts: list[Any] = []
    position = 0
    for match in _REFERENCE.finditer(text):
        if match.start() > position:
            parts.append(text[position : match.start()])
        parts.append(_Reference(match.group("body")))
        position = match.end()
    if position < len(text):
        parts.append(text[position:])
    if not any(isinstance(part, _Reference) for part in parts):
        # `{{` with no closing braces, e.g. a stray literal — no references.
        return _Static(text)
    return _Interpolation(tuple(parts))


def compile_cache_info() -> Any:
    """Hits/misses for the string cache. Used by tests and the benchmark."""
    return compile_string.cache_info()


def clear_compile_cache() -> None:
    compile_string.cache_clear()
