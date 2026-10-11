import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("package-licenses.py")
SPEC = importlib.util.spec_from_file_location("package_licenses", SCRIPT)
package_licenses = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(package_licenses)


class PackageLicensesTests(unittest.TestCase):
    def test_parse_stream_deduplicates_dependencies_and_skips_main(self):
        stream = "\n".join(
            json.dumps(item)
            for item in [
                {"ImportPath": "main", "Module": {"Path": "example/main", "Main": True}},
                {"ImportPath": "dep/a", "Module": {"Path": "example/a", "Version": "v1.2.3", "Sum": "h1:abc"}},
                {"ImportPath": "dep/b", "Module": {"Path": "example/a", "Version": "v1.2.3", "Sum": "h1:abc"}},
                {"ImportPath": "dep/c", "Module": {"Path": "example/b", "Version": "v2.0.0", "Replace": {"Path": "fork/b", "Version": "v2.1.0", "Dir": "/tmp/fork"}}},
            ]
        )
        modules = package_licenses.parse_go_list(stream)
        self.assertEqual([(m["Path"], m["Version"]) for m in modules], [("example/a", "v1.2.3"), ("example/b", "v2.0.0")])
        self.assertEqual(modules[1]["Replace"]["Path"], "fork/b")

    def test_collects_all_notices_and_records_replacement_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            effective = root / "forked-module"
            (effective / "crypto" / "olm").mkdir(parents=True)
            (effective / "LICENSE").write_bytes(b"upstream license\n")
            (effective / "crypto" / "olm" / "COPYING").write_bytes(b"sub-package notice\n")
            module = {
                "Path": "example.org/original",
                "Version": "v1.0.0",
                "Sum": "h1:requested-checksum",
                "GoModSum": "h1:gomod-checksum",
                "Dir": str(root / "unused-original"),
                "Replace": {"Path": "fork.example.org/replacement", "Version": "v1.1.0", "Dir": str(effective)},
            }
            output = root / "release-notices"
            package_licenses.generate(output, [module, module])
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(len(manifest["modules"]), 1)
            entry = manifest["modules"][0]
            self.assertEqual(entry["path"], "example.org/original")
            self.assertEqual(entry["version"], "v1.0.0")
            self.assertEqual(entry["sum"], "h1:requested-checksum")
            self.assertEqual(entry["replacement"]["path"], "fork.example.org/replacement")
            self.assertEqual(entry["source_url"], "https://proxy.golang.org/fork.example.org/replacement/@v/v1.1.0.zip")
            self.assertEqual(entry["requested_source_url"], "https://proxy.golang.org/example.org/original/@v/v1.0.0.zip")
            paths = {item["path"] for item in entry["license_files"]}
            self.assertEqual(len(paths), 2)
            self.assertTrue(any(path.endswith("/LICENSE") for path in paths))
            self.assertTrue(any(path.endswith("/crypto/olm/COPYING") for path in paths))
            self.assertIn(b"upstream license\n", (output / next(path for path in paths if path.endswith("/LICENSE"))).read_bytes())
            self.assertIn("not legal advice", (output / "README.md").read_text())

    def test_missing_license_fails_without_publishing_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "unlicensed"
            source.mkdir()
            (source / "code.go").write_text("package x\n")
            output = root / "notices"
            with self.assertRaisesRegex(package_licenses.PackageError, "example.org/unlicensed") as raised:
                package_licenses.generate(output, [{"Path": "example.org/unlicensed", "Version": "v1.0.0", "Dir": str(source)}])
            self.assertNotIn(str(source), str(raised.exception))
            self.assertFalse(output.exists())
            self.assertEqual(list(root.glob(".notices.staging-*")), [])

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "existing"
            output.mkdir()
            marker = output / "keep.txt"
            marker.write_text("keep")
            with self.assertRaisesRegex(package_licenses.PackageError, "already exists"):
                package_licenses.generate(output, [])
            self.assertEqual(marker.read_text(), "keep")


if __name__ == "__main__":
    unittest.main()
