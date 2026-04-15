import os
import unittest
from unittest.mock import patch

from src.config import SqlServerConfig, SqlServerConfigError


class SqlServerConfigTests(unittest.TestCase):
    def test_load_trims_values_and_applies_defaults(self) -> None:
        env = {
            "SQL_SERVER_HOST": " sql-host ",
            "SQL_SERVER_USERNAME": " app_user ",
            "SQL_SERVER_PASSWORD": " secret ",
        }

        with patch.dict(os.environ, env, clear=False):
            config = SqlServerConfig.load()

        self.assertEqual(config.host, "sql-host")
        self.assertEqual(config.username, "app_user")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.port, 1433)
        self.assertEqual(config.database, "HILLSBORO_Audit")
        self.assertEqual(config.driver, "ODBC Driver 18 for SQL Server")
        self.assertEqual(config.encrypt, "yes")
        self.assertEqual(config.trust_server_certificate, "yes")
        self.assertEqual(config.timeout, 30)

    def test_load_raises_clear_error_when_required_values_are_missing(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SQL_SERVER_HOST": " ",
                "SQL_SERVER_USERNAME": "",
            },
            clear=True,
        ):
            with self.assertRaises(SqlServerConfigError) as context:
                SqlServerConfig.load()

        self.assertIn("SQL_SERVER_HOST", str(context.exception))
        self.assertIn("SQL_SERVER_USERNAME", str(context.exception))
        self.assertIn("SQL_SERVER_PASSWORD", str(context.exception))


if __name__ == "__main__":
    unittest.main()
