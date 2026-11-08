from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from urllib.error import HTTPError, URLError
ROOT=Path(__file__).resolve().parent.parent
class ConsoleTests(unittest.TestCase):
    def test_read_only_routes(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest=Path(temp)/'manifest.json';manifest.write_text('{"coverage":"incomplete"}')
            proc=subprocess.Popen([sys.executable,str(ROOT/'replay-console/server.py'),'--manifest',str(manifest),'--port','4793'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            try:
                for _ in range(100):
                    try:r=urllib.request.urlopen('http://127.0.0.1:4793/manifest.json',timeout=1);break
                    except URLError:
                        if proc.poll() is not None:self.fail('Console server exited')
                        time.sleep(.02)
                else:self.fail('Server startup timed out')
                self.assertEqual(r.read(),b'{"coverage": "incomplete"}')
                self.assertEqual(r.headers['X-Content-Type-Options'],'nosniff')
                r.close()
                for path in ['/../collector/eventweave_collector.py','/unknown']:
                    with self.assertRaises(HTTPError) as error:urllib.request.urlopen('http://127.0.0.1:4793'+path)
                    self.assertEqual(error.exception.code,404)
                    error.exception.close()
                request=urllib.request.Request('http://127.0.0.1:4793/manifest.json',data=b'{}',method='POST')
                with self.assertRaises(HTTPError) as error:urllib.request.urlopen(request)
                self.assertEqual(error.exception.code,501)
                error.exception.close()
            finally:proc.terminate();proc.wait(timeout=5)
