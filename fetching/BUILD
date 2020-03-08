python_binary(
  name='fetch-article',
  source='fetch-article.py',
  dependencies=[
    '3rdparty/python:beautifulsoup4',
    '3rdparty/python:lxml',
    '3rdparty/python:newspaper3k',
    '3rdparty/python:pants',
    '3rdparty/python:python-dateutil',
    '3rdparty/python:requests',
    '3rdparty/python:requests-futures',
    '3rdparty/python:thrift',
    ':article-fetch-thrift-python',
  ],
  compatibility=['CPython>=3.8'],
)

java_thrift_library(
  name='article-fetch-thrift-java',
  sources=['*.thrift'],
)


python_thrift_library(
  name='article-fetch-thrift-python',
  sources=['*.thrift'],
)
