import pathlib
import re
import unittest


class CloudRuntimePinsTest(unittest.TestCase):
    EXPECTED_LINES = [
        "streamlit==1.54.0",
        "pandas==2.3.3",
        "numpy==2.2.6",
        "pyarrow==20.0.0",
        "protobuf==5.29.5",
        "altair==5.5.0",
        "requests==2.34.2",
        "beautifulsoup4==4.15.0",
    ]

    def setUp(self):
        req_path = pathlib.Path(__file__).resolve().parents[1] / "requirements.txt"
        self.lines = [
            line.strip()
            for line in req_path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    def test_required_cloud_packages_are_exactly_pinned(self):
        self.assertEqual(self.lines, self.EXPECTED_LINES)

    def test_requirements_has_no_range_or_wildcard_pins(self):
        content = "\n".join(self.lines)

        self.assertNotRegex(content, re.compile(r">=|<=|~=|\*"))
        for line in self.lines:
            self.assertEqual(line.count("=="), 1, line)

    def test_native_server_stack_is_not_directly_required(self):
        package_names = {line.split("==", 1)[0].lower() for line in self.lines}

        self.assertNotIn("uvicorn", package_names)
        self.assertNotIn("starlette", package_names)
        self.assertNotIn("httptools", package_names)


if __name__ == "__main__":
    unittest.main()
