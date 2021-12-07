Tests for the Scylla API.

Tests use the requests library and the pytest frameworks
(both are available from Linux distributions, or with "pip install").

To run all tests against the local installation of scylla that listens on
http://localhost:10000, just run `pytest`.

Some additional pytest options:
* To run all tests in a single file, do `pytest test_foo.py`.
* To run a single specific test, do `pytest test_foo.py::test_bar`.
* Additional useful pytest options, especially useful for debugging tests:
  * -v: show the names of each individual test running instead of just dots.
  * -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)
