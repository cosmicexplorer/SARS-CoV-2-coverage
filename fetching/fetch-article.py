import datetime
import json
import os
import queue
import re
import sys
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from time import mktime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse, ParseResult

import dateutil
import lxml.html
import requests
from bs4 import BeautifulSoup  # type: ignore
from newspaper import Article  # type: ignore
from pants.util.memo import memoized_classproperty  # type: ignore
from requests.models import Response
from requests_futures.sessions import FuturesSession  # type: ignore
from thrift.TSerialization import serialize as thrift_serialize
from thrift.protocol.TJSONProtocol import TSimpleJSONProtocolFactory

from fetching.article_fetch.thrift import ttypes as fetch_thrift


def thrift_json_serialize(thrift_object) -> bytes:
  return thrift_serialize(
    thrift_object,
    protocol_factory=TSimpleJSONProtocolFactory(),
  )


TWITTER_PLAINTEXT_SEARCH_BASE_URL = 'https://mobile.twitter.com'


@dataclass(frozen=True)
class TwitterSearchUrl:
  url: str

  @classmethod
  def from_relative_path(cls, path: str) -> 'TwitterSearchUrl':
    assert path.startswith('/search?q=')
    return cls(
      f'{TWITTER_PLAINTEXT_SEARCH_BASE_URL}{path}')


@dataclass(frozen=True)
class TwitterSearchShortenedUrl:
  url: str

  TWEET_SHORTENED_LINK_BASE = 'https://t.co/'

  def __post_init__(self) -> None:
    assert self.url.startswith(self.TWEET_SHORTENED_LINK_BASE)


@dataclass
class Tags:
  tags: List[str]
  meta_description: Optional[str]
  meta_keywords: List[str]

  @classmethod
  def filter_tags(cls, tags: Iterable[str]) -> Iterable[str]:
    for t in tags:
      if t := cls.filter_single_tag(t):
        yield t

  @classmethod
  def filter_single_tag(cls, tag: Optional[str]) -> Optional[str]:
    if tag:
      return tag
    return None

  def __init__(self,
               tags: Iterable[str],
               meta_description: Optional[str],
               meta_keywords: Iterable[str],
               ) -> None:
    self.tags = list(self.filter_tags(tags))
    self.meta_description = self.filter_single_tag(meta_description)
    self.meta_keywords = list(self.filter_tags(meta_keywords))

  def into_thrift(self) -> fetch_thrift.Tags:
    return fetch_thrift.Tags(
      tags=self.tags,
      meta_description=self.meta_description,
      meta_keywords=self.meta_keywords,
    )


@dataclass(frozen=True)
class LinkFromArticle:
  scheme: Optional[str]
  netloc: Optional[str]
  path: Optional[str]

  @classmethod
  def parse_url(cls, url: str) -> Optional['LinkFromArticle']:
    scheme, netloc, path, _params, _query, _fragment = urlparse(url)
    if not any([scheme, netloc, path]):
      return None
    return cls(
      scheme=(scheme if scheme else None),
      netloc=(netloc if netloc else None),
      path=(path if path else None),
    )

  def resolve_from(self, base: ParseResult) -> Optional['ResolvedSubLink']:
    scheme = self.scheme
    if not scheme:
      if not self.netloc:
        scheme = base.scheme
      else:
        scheme = 'https'

    if not re.match('https?', scheme):
      return None

    # If we were given an absolute url:
    netloc = self.netloc
    if netloc:
      if self.path:
        assert self.path.startswith('/')
        path = self.path
      else:
        path = '/'
    else:
      netloc = base.netloc
      # If there was no netloc or path, e.g. a '#a' fragment link leading to the same page.
      if not self.path:
        return None
      if self.path.startswith('/'):
        path = self.path
      else:
        base_dir = os.path.dirname(base.path)
        path_from_root = os.path.join(base_dir, self.path)
        path = f'/{path_from_root}'

    return ResolvedSubLink(
      scheme=scheme,
      netloc=netloc,
      path=path,
    )


@dataclass(frozen=True)
class ResolvedSubLink:
  scheme: str
  netloc: str
  path: str

  def __post_init__(self) -> None:
    assert all([self.scheme, self.netloc, self.path.startswith('/')])

  @classmethod
  def from_url(cls, url: str) -> Optional['ResolvedSubLink']:
    scheme, netloc, path, _, _, _ = urlparse(url)
    if not all([scheme, netloc, path]):
      return None
    return cls(
      scheme=scheme,
      netloc=netloc,
      path=path,
    )

  def into_thrift(self) -> fetch_thrift.URL:
    return fetch_thrift.URL(
      scheme=self.scheme,
      netloc=self.netloc,
      path=self.path,
    )

  def into_url(self) -> str:
    return f'{self.scheme}://{self.netloc}{self.path}'


@dataclass(frozen=True)
class LinksOnPage:
  links: List[ResolvedSubLink]

  def into_thrift(self) -> List[fetch_thrift.URL]:
    return [l.into_thrift() for l in self.links]

  @classmethod
  def from_article_html(cls, article: Article) -> 'LinksOnPage':
    parsed_base_url = urlparse(article.url)
    if parsed_base_url.scheme:
      assert re.match('https?', parsed_base_url.scheme)

    links = []
    for _, _, sub_url, _ in lxml.html.iterlinks(article.html):
      if sub_link := LinkFromArticle.parse_url(sub_url):
        if resolved_link := sub_link.resolve_from(parsed_base_url):
          links.append(resolved_link)

    return cls(links)


@dataclass(frozen=True)
class NewsArticle:
  url: str
  title: str
  authors: List[str]
  tags: Tags
  links: LinksOnPage
  publish_date: datetime.datetime
  text: str

  PAGE_NOT_FOUND_TITLE_MARKER = 'Page Not Found'

  @classmethod
  def from_response(cls, resp: Response) -> Optional['NewsArticle']:
    content_type = resp.headers['Content-Type']
    if not content_type.startswith('text/html'):
      return None

    article = Article(resp.url)
    article.set_html(resp.content)
    article.parse()

    if (not article.title) or article.title == cls.PAGE_NOT_FOUND_TITLE_MARKER:
      return None

    if not article.authors:
      return None

    most_specific_possible_date = article.publish_date
    # Sometimes, a more specific timestamp may be provided from the article metadata, e.g. from
    # 'http://fox13now.com/2013/12/30/new-year-new-laws-obamacare-pot-guns-and-drones/'.
    if metadata := article.meta_data.get('article', None):
      if date_string := metadata.get('published_time', None):
        most_specific_possible_date = dateutil.parser.parse(date_string)
    if not most_specific_possible_date:
      return None

    if not article.text:
      return None

    links = LinksOnPage.from_article_html(article)

    return cls(url=resp.url,
               title=article.title,
               authors=article.authors,
               tags=Tags(
                 tags=list(article.tags),
                 meta_description=article.meta_description,
                 meta_keywords=article.meta_keywords,
               ),
               links=links,
               publish_date=most_specific_possible_date,
               text=article.text)

  def into_thrift(self) -> fetch_thrift.Article:
    fetch_id = fetch_thrift.TransientFetchId(str(uuid.uuid4()))
    sub_link = ResolvedSubLink.from_url(self.url)
    assert sub_link is not None
    return fetch_thrift.Article(
      fetch_id=fetch_id,
      url=sub_link.into_thrift(),
      title=self.title,
      authors=self.authors,
      tags=self.tags.into_thrift(),
      links=self.links.into_thrift(),
      publish_timestamp=int(mktime(self.publish_date.timetuple())),
      text=self.text,
    )

  def __str__(self) -> str:
    thrift_object = self.into_thrift()
    ret = json.loads(thrift_json_serialize(thrift_object))
    ret['url'] = self.url
    # Don't show the whole article text, it could be huge!
    ret['text'] = ret['text'][0:50] + '...'
    ret['links'] = ['...']
    ret['uuid'] = ret['fetch_id']['uuid']
    del ret['fetch_id']
    return json.dumps(ret)


@dataclass(frozen=True)
class ExternalUrlFetchSet:
  urls: List[TwitterSearchShortenedUrl]

  @memoized_classproperty
  def _session(cls) -> FuturesSession:
    return FuturesSession(executor=ThreadPoolExecutor(max_workers=10))

  def scramble_the_jets(self) -> List[Future]:
    return [self._session.get(url.url) for url in self.urls]


@dataclass(frozen=True)
class TwitterSearchCursor:
  next_url: TwitterSearchUrl
  t_co_urls: ExternalUrlFetchSet

  @memoized_classproperty
  def _session(cls) -> FuturesSession:
    return FuturesSession(executor=ThreadPoolExecutor(max_workers=10))

  @classmethod
  def _extract_next_search_page_url(cls, page: BeautifulSoup) -> TwitterSearchUrl:
    next_url_button = page.find('div', class_='w-button-more')
    next_url = next_url_button.find('a')['href']
    return TwitterSearchUrl.from_relative_path(next_url)

  @classmethod
  def _extract_t_co_urls(cls, page: BeautifulSoup) -> Iterable[TwitterSearchShortenedUrl]:
    for tweet in page.find_all('table', class_='tweet'):
      for a in tweet.find_all('a'):
        url = a['href']

        # Only fetch external links.
        if url.startswith('/'):
          continue

        yield TwitterSearchShortenedUrl(url)

  @classmethod
  def from_base_url(cls, base_url: TwitterSearchUrl) -> 'TwitterSearchCursor':
    fetched_page = cls._session.get(base_url.url).result()
    page = BeautifulSoup(fetched_page.content, features='lxml')

    next_url = cls._extract_next_search_page_url(page)

    t_co_urls = ExternalUrlFetchSet(list(cls._extract_t_co_urls(page)))

    return cls(next_url=next_url, t_co_urls=t_co_urls)


@dataclass(frozen=True)
class TwitterSearchQuery:
  alternating_keywords: List[str]

  TWITTER_DOMAIN = 'twitter.com'

  def _as_initial_query(self) -> str:
    joined_keywords = ' OR '.join(self.alternating_keywords)
    return f'/search?q=({joined_keywords})'

  def _paged_fetch_cursors(self) -> Iterable[TwitterSearchCursor]:
    cur_page = TwitterSearchUrl.from_relative_path(self._as_initial_query())

    while True:
      cursor = TwitterSearchCursor.from_base_url(cur_page)
      yield cursor
      cur_page = cursor.next_url

  def paged_fetch_the_news(self) -> Iterable[NewsArticle]:

    cursors = queue.Queue()

    # At most X concurrent downloads at once!
    # TODO: how should we select this number???
    article_fetch_futures = queue.Queue(maxsize=50)

    def cursor_fn():
      for cursor in self._paged_fetch_cursors():
        for fetch_future in cursor.t_co_urls.scramble_the_jets():
          article_fetch_futures.put(fetch_future)

    cursor_thread = threading.Thread(
      name='Twitter Search Cursor Paging Producer',
      target=cursor_fn,
    )
    cursor_thread.start()

    while True:
      new_fetch_futures = []
      try:
        while True:
          new_fetch_futures.append(article_fetch_futures.get(block=False))
      except queue.Empty:
        pass
      # If there were no new fetch futures, wait until there is one.
      if not new_fetch_futures:
        new_fetch_futures.append(article_fetch_futures.get())

      for fetch_future in as_completed(new_fetch_futures):
        try:
          resp = fetch_future.result()
        except requests.exceptions.SSLError:
          continue

        # Get the final url, after redirection.
        # Only fetch links pointing away from the twitter site.
        if self.TWITTER_DOMAIN in resp.url:
          continue

        if article := NewsArticle.from_response(resp):
          yield article


def main():
  twitter_search_query = TwitterSearchQuery(
    alternating_keywords=[
      'coronavirus',
      'sars-cov-2',
      'covid-19',
    ],
  )

  sys.stderr.write(f'executing query {twitter_search_query}...\n')

  for article in twitter_search_query.paged_fetch_the_news():
    sys.stderr.write(f'article found: {article}\n')
    article_thrift_message = thrift_json_serialize(article.into_thrift())
    sys.stdout.write(article_thrift_message.decode() + '\n')


if __name__ == '__main__':
  main()
