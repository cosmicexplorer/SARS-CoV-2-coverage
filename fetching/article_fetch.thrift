#@namespace scala fetching.article_fetch.thrift
namespace py fetching.article_fetch.thrift

struct TransientFetchId {
  1: optional string uuid,
}

struct URL {
  1: optional string scheme,
  2: optional string netloc,
  3: optional string path,
}

struct Tags {
  1: optional list<string> tags,
  # meta_description and meta_keywords are "truly optional" here
  2: optional string meta_description,
  3: optional list<string> meta_keywords,
}

struct Article {
  1: optional TransientFetchId fetch_id,
  2: optional URL url,
  3: optional string title,
  4: optional list<string> authors,
  5: optional Tags tags,
  6: optional list<URL> links,
  7: optional i64 publish_timestamp,
  8: optional string text,
}
