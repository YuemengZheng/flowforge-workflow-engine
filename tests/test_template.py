"""Template compilation: same answers as the reference resolver, less work.

The compiler's whole risk is staleness — a plan is reused across attempts, waves
and runs, so it must hold no values, only structure. ``EquivalenceTests`` pins
the behaviour against ``resolve_uncached`` on a table of awkward inputs rather
than on cases the compiler was written to handle.
"""

import unittest

from flowforge import Graph, VariableError, VariablePool, WorkflowEngine
from flowforge.variables import (
    Template,
    clear_compile_cache,
    compile_cache_info,
    compile_string,
    compile_template,
)


def populated_pool() -> VariablePool:
    pool = VariablePool({"question": "why", "n": 3})
    pool.set_outputs("fetch", {"name": "ada", "rows": [{"id": 7}, {"id": 8}], "ok": True})
    pool.set_outputs("score", {"value": 0.5, "missing": None})
    return pool


# Values that must resolve identically through both implementations.
CASES = [
    "no references here",
    "",
    "{{fetch.name}}",
    "  {{fetch.name}}  ",
    "hello {{fetch.name}}",
    "{{fetch.name}} and {{inputs.question}}",
    "{{fetch.rows}}",
    "{{fetch.rows.0.id}}",
    "id is {{fetch.rows.1.id}}",
    "{{fetch.ok}}",
    "flag: {{fetch.ok}}",
    "{{score.value}}",
    "n={{inputs.n}}",
    "{{score.missing}}",
    "empty: {{score.missing}}",
    "{{nope.x ?? \"n/a\"}}",
    "{{nope.x ?? 42}}",
    "{{nope.x ?? [1, 2]}}",
    "{{fetch.name ?? \"unused\"}}",
    "text {{nope.x ?? \"fb\"}} more",
    "{{ fetch.name }}",
    "a {{fetch.name}} b {{fetch.rows.0.id}} c",
    "{{fetch.name}}{{inputs.question}}",
    "literal {{ braces",
    {"a": 1, "b": "static"},
    {"a": "{{fetch.name}}", "b": {"c": "{{inputs.n}}", "d": 4}},
    {"list": ["{{fetch.name}}", 2, {"deep": "{{score.value}}"}]},
    ["{{fetch.name}}", "plain"],
    ("{{fetch.name}}", "plain"),
    {"empty_dict": {}, "empty_list": []},
    [],
    {},
    42,
    None,
    True,
    3.5,
]

ERROR_CASES = [
    "{{ghost.x}}",
    "{{fetch.nothere}}",
    "{{fetch.rows.9.id}}",
    "{{fetch.rows.abc}}",
    "{{fetch.name.deeper}}",
    "{{  }}",
    "prefix {{ghost.x}} suffix",
    {"nested": ["{{ghost.x}}"]},
]


class EquivalenceTests(unittest.TestCase):
    def setUp(self):
        self.pool = populated_pool()

    def test_compiled_and_uncached_agree(self):
        for value in CASES:
            with self.subTest(value=value):
                self.assertEqual(
                    self.pool.resolve(value), self.pool.resolve_uncached(value)
                )

    def test_failures_agree_message_for_message(self):
        for value in ERROR_CASES:
            with self.subTest(value=value):
                with self.assertRaises(VariableError) as compiled:
                    self.pool.resolve(value)
                with self.assertRaises(VariableError) as reference:
                    self.pool.resolve_uncached(value)
                self.assertEqual(str(compiled.exception), str(reference.exception))

    def test_a_whole_reference_keeps_its_type(self):
        self.assertEqual(self.pool.resolve("{{fetch.rows}}"), [{"id": 7}, {"id": 8}])
        self.assertIsInstance(self.pool.resolve("{{fetch.ok}}"), bool)
        self.assertIsInstance(self.pool.resolve("{{inputs.n}}"), int)
        # Embedded, the same reference is stringified.
        self.assertEqual(self.pool.resolve("flag {{fetch.ok}}"), "flag true")


class StalenessTests(unittest.TestCase):
    """A plan holds structure only. If it ever held a value, these would fail."""

    def test_one_plan_renders_differently_as_the_pool_changes(self):
        plan = compile_template({"who": "{{fetch.name}}"})
        pool = VariablePool()

        pool.set_outputs("fetch", {"name": "ada"})
        self.assertEqual(plan.render(pool), {"who": "ada"})

        pool.set_outputs("fetch", {"name": "grace"})
        self.assertEqual(plan.render(pool), {"who": "grace"})

    def test_a_cached_string_plan_is_not_bound_to_the_first_pool(self):
        clear_compile_cache()
        first = VariablePool({"x": 1})
        second = VariablePool({"x": 2})

        self.assertEqual(first.resolve("{{inputs.x}}"), 1)
        self.assertEqual(second.resolve("{{inputs.x}}"), 2)
        # Same compiled plan served both.
        self.assertIs(compile_string("{{inputs.x}}"), compile_string("{{inputs.x}}"))

    def test_a_missing_reference_still_raises_on_a_later_render(self):
        plan = compile_template("{{fetch.name}}")
        pool = VariablePool()
        with self.assertRaises(VariableError):
            plan.render(pool)
        pool.set_outputs("fetch", {"name": "ada"})
        self.assertEqual(plan.render(pool), "ada")


class StaticShortCircuitTests(unittest.TestCase):
    def test_a_template_free_container_renders_as_itself(self):
        # The documented read-only contract: no copy is made, which is where
        # most of the saving on a plain config comes from.
        config = {"model": "opus", "limits": {"tokens": 10}, "tags": ["a", "b"]}
        self.assertIs(VariablePool().resolve(config), config)

    def test_a_container_with_one_reference_is_rebuilt(self):
        config = {"model": "opus", "who": "{{inputs.name}}"}
        pool = VariablePool({"name": "ada"})
        resolved = pool.resolve(config)

        self.assertIsNot(resolved, config)
        self.assertEqual(resolved, {"model": "opus", "who": "ada"})
        self.assertEqual(config["who"], "{{inputs.name}}")  # source untouched

    def test_static_plans_report_themselves_as_static(self):
        self.assertTrue(compile_template({"a": [1, 2, {"b": "c"}]}).static)
        self.assertFalse(compile_template({"a": ["{{x.y}}"]}).static)
        self.assertTrue(compile_template("no braces").static)
        self.assertTrue(compile_template("stray {{ brace").static)
        self.assertFalse(compile_template("{{x.y}}").static)

    def test_every_plan_is_a_template(self):
        for value in CASES:
            with self.subTest(value=value):
                self.assertIsInstance(compile_template(value), Template)


class CacheTests(unittest.TestCase):
    def setUp(self):
        clear_compile_cache()

    def test_a_repeated_string_is_compiled_once(self):
        compile_string("{{a.b}} and {{c.d}}")
        first = compile_cache_info()
        compile_string("{{a.b}} and {{c.d}}")
        second = compile_cache_info()

        self.assertEqual(first.misses, 1)
        self.assertEqual(second.misses, 1)
        self.assertEqual(second.hits, 1)

    def test_rendering_does_not_recompile(self):
        pool = populated_pool()
        plan = compile_template({"who": "{{fetch.name}}", "n": "{{inputs.n}}"})
        misses_after_compile = compile_cache_info().misses

        for _ in range(50):
            plan.render(pool)

        self.assertEqual(compile_cache_info().misses, misses_after_compile)

    def test_the_cache_is_bounded(self):
        self.assertEqual(compile_cache_info().maxsize, 8192)


class EnginePlanTests(unittest.IsolatedAsyncioTestCase):
    """The engine compiles at construction and renders per attempt."""

    async def test_two_runs_of_one_engine_do_not_share_values(self):
        graph = Graph.from_dict(
            {
                "id": "greet",
                "nodes": [
                    {"id": "start", "type": "start"},
                    {
                        "id": "say",
                        "type": "stub",
                        "config": {"output": {"greeting": "hi {{start.who}}"}},
                    },
                ],
                "edges": [{"source": "start", "target": "say"}],
            }
        )
        engine = WorkflowEngine(graph)

        first = await engine.run({"who": "ada"})
        second = await engine.run({"who": "grace"})

        self.assertEqual(first.outputs_of("say")["greeting"], "hi ada")
        self.assertEqual(second.outputs_of("say")["greeting"], "hi grace")

    async def test_the_spec_is_never_edited_by_a_run(self):
        graph = Graph.from_dict(
            {
                "id": "keep",
                "nodes": [
                    {"id": "start", "type": "start"},
                    {
                        "id": "say",
                        "type": "stub",
                        "config": {"output": {"greeting": "hi {{start.who}}"}},
                    },
                ],
                "edges": [{"source": "start", "target": "say"}],
            }
        )
        before = repr(graph.nodes["say"].config)
        await WorkflowEngine(graph).run({"who": "ada"})

        self.assertEqual(repr(graph.nodes["say"].config), before)

    async def test_a_raw_config_key_survives_compilation(self):
        """`iterate` hands its nested workflow to the sub-run untouched."""
        graph = Graph.from_dict(
            {
                "id": "loop",
                "nodes": [
                    {
                        "id": "each",
                        "type": "iterate",
                        "config": {
                            "items": [1, 2],
                            "workflow": {
                                "nodes": [
                                    {
                                        "id": "work",
                                        "type": "stub",
                                        "config": {
                                            "output": {"v": "{{inputs.item}}"}
                                        },
                                    }
                                ]
                            },
                        },
                    }
                ],
            }
        )
        result = await WorkflowEngine(graph).run()

        self.assertEqual(
            [item["v"] for item in result.outputs_of("each")["results"]],
            [1, 2],
        )


if __name__ == "__main__":
    unittest.main()
