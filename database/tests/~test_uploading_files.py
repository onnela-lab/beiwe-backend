from flask import Flask, Request, request
from io import StringIO
import unittest

RESULT = False

class TestFileFail(unittest.TestCase):

    def test_1_uploading(self):
        class FileObj(StringIO):

            def close(self):
                print('in file close')
                global RESULT
                RESULT = True

        class MyRequest(Request):
            def _get_file_stream(self, *args, **kwargs):
                return FileObj()

        app = Flask(__name__)
        app.debug = True
        app.request_class = MyRequest

        @app.route("/upload_gps", methods=['POST'])
        def upload():
            f = request.files['file']
            self.assertIsInstance(
                f.stream,
                FileObj,
            )
            f.close()
            return 'ok'

        client = app.test_client()
        resp = client.post(
            '/upload',
            data = {
                'file': (StringIO('my file contents, texting'), 'hello world.csv'),
            }
        )
        self.assertEqual(
            'ok',
            resp.data,
        )
        global RESULT
        self.assertTrue(RESULT)


if __name__ == '__main__':
    unittest.main()
