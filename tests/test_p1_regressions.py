import unittest

from app.services.dag_generator import generate_dag_content
from app.services.pipeline_validator import validate_sql_query


class P1RegressionTests(unittest.TestCase):
    def test_rejects_multi_statement_sql(self):
        query = "DROP TABLE mart.x; CREATE TABLE mart.x AS SELECT 1"
        self.assertEqual(validate_sql_query(query), "Query mengandung pola SQL berbahaya")

    def test_accepts_single_create_as_query(self):
        query = "CREATE TABLE mart.x AS SELECT * FROM raw.source"
        self.assertIsNone(validate_sql_query(query))

    def test_generated_sql_has_quality_gate_and_idempotent_replace(self):
        content = generate_dag_content(
            "p1_test",
            "P1 Test",
            [{
                "name": "Build mart",
                "query_type": "sql",
                "query": "CREATE TABLE mart.output AS SELECT * FROM raw.source",
                "source_table": "raw.source",
                "dest_table": "mart.output",
            }],
        )
        self.assertIn("SELECT COUNT(*) FROM raw.source", content)
        self.assertIn("DROP TABLE IF EXISTS mart.output", content)
        self.assertIn("CREATE TABLE mart.output AS", content)


if __name__ == "__main__":
    unittest.main()
