import unittest

from flowforge import VariableError, VariablePool


class LookupTests(unittest.TestCase):
    def setUp(self):
        self.pool = VariablePool({"question": "why", "n": 2})
        self.pool.set_outputs(
            "fetch",
            {"name": "ada", "rows": [{"id": 1}, {"id": 2}], "meta": {"ok": True}},
        )

    def test_run_inputs_live_under_inputs(self):
        self.assertEqual(self.pool.get("inputs.question"), "why")

    def test_node_output_field(self):
        self.assertEqual(self.pool.get("fetch.name"), "ada")

    def test_nested_field(self):
        self.assertIs(self.pool.get("fetch.meta.ok"), True)

    def test_list_index(self):
        self.assertEqual(self.pool.get("fetch.rows.1.id"), 2)

    def test_unknown_namespace_lists_what_exists(self):
        with self.assertRaises(VariableError) as ctx:
            self.pool.get("ghost.x")
        self.assertIn("fetch", str(ctx.exception))

    def test_unknown_field(self):
        with self.assertRaises(VariableError):
            self.pool.get("fetch.nope")

    def test_index_out_of_range(self):
        with self.assertRaises(VariableError):
            self.pool.get("fetch.rows.9")

    def test_non_numeric_index_into_list(self):
        with self.assertRaises(VariableError):
            self.pool.get("fetch.rows.name")

    def test_walking_into_a_scalar(self):
        with self.assertRaises(VariableError):
            self.pool.get("fetch.name.length")


class ResolutionTests(unittest.TestCase):
    def setUp(self):
        self.pool = VariablePool({"topic": "graphs"})
        self.pool.set_outputs("a", {"n": 7, "rows": [1, 2], "flag": False, "nil": None})

    def test_whole_string_reference_keeps_the_type(self):
        self.assertEqual(self.pool.resolve("{{a.rows}}"), [1, 2])
        self.assertEqual(self.pool.resolve("{{ a.n }}"), 7)
        self.assertIs(self.pool.resolve("{{a.flag}}"), False)

    def test_embedded_reference_is_interpolated(self):
        self.assertEqual(
            self.pool.resolve("write about {{inputs.topic}} in {{a.n}} words"),
            "write about graphs in 7 words",
        )

    def test_embedded_structures_are_json(self):
        self.assertEqual(self.pool.resolve("rows={{a.rows}}"), "rows=[1, 2]")

    def test_embedded_none_is_empty_string(self):
        self.assertEqual(self.pool.resolve("[{{a.nil}}]"), "[]")

    def test_nested_containers_are_resolved(self):
        resolved = self.pool.resolve(
            {"prompt": "on {{inputs.topic}}", "limits": [{"max": "{{a.n}}"}]}
        )
        self.assertEqual(resolved, {"prompt": "on graphs", "limits": [{"max": 7}]})

    def test_non_string_values_pass_through(self):
        self.assertEqual(self.pool.resolve({"delay": 0.5, "on": True}), {"delay": 0.5, "on": True})

    def test_missing_reference_raises(self):
        with self.assertRaises(VariableError):
            self.pool.resolve("{{ghost.x}}")

    def test_fallback_is_used_when_missing(self):
        self.assertEqual(self.pool.resolve('{{ghost.x ?? "none"}}'), "none")
        self.assertEqual(self.pool.resolve("{{ghost.x ?? 42}}"), 42)
        self.assertEqual(self.pool.resolve("owner {{ghost.x ?? \"n/a\"}}"), "owner n/a")

    def test_fallback_is_ignored_when_present(self):
        self.assertEqual(self.pool.resolve('{{a.n ?? "none"}}'), 7)

    def test_references_are_discoverable(self):
        found = set(self.pool.references({"p": "{{a.n}} and {{ghost.y ?? 1}}"}))
        self.assertEqual(found, {"a.n", "ghost.y"})


class IsolationTests(unittest.TestCase):
    def test_pools_do_not_share_state(self):
        first, second = VariablePool({"x": 1}), VariablePool({"x": 2})
        first.set_outputs("a", {"v": "left"})
        self.assertEqual(first.get("a.v"), "left")
        with self.assertRaises(VariableError):
            second.get("a.v")

    def test_snapshot_is_a_copy(self):
        pool = VariablePool({"x": 1})
        pool.set_outputs("a", {"v": 1})
        snapshot = pool.snapshot()
        snapshot["a"]["v"] = 999
        self.assertEqual(pool.get("a.v"), 1)


if __name__ == "__main__":
    unittest.main()
