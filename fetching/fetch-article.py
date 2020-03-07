import datetime
import json
import queue
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup  # type: ignore
from newspaper import Article  # type: ignore
from pants.util.memo import memoized_classproperty  # type: ignore
from requests.models import Response
from requests_futures.sessions import FuturesSession  # type: ignore

TWITTER_PLAINTEXT_SEARCH_BASE_URL = 'https://mobile.twitter.com'


@dataclass(frozen=True)
class TwitterSearchUrl:
  url: str

  @classmethod
  def from_relative_path(cls, path: str) -> 'TwitterSearchUrl':
    assert path.startswith('/search?q=')
    return cls(
      f'{TWITTER_PLAINTEXT_SEARCH_BASE_URL}{path}')


@dataclass
class TwitterSearchShortenedUrl:
  url: str

  TWEET_SHORTENED_LINK_BASE = 'https://t.co/'

  def __init__(self, url: str) -> None:
    assert url.startswith(self.TWEET_SHORTENED_LINK_BASE)
    self.url = url


@dataclass(frozen=True)
class NewsArticle:
  url: str
  title: str
  authors: List[str]
  publish_date: datetime.datetime
  text: str

  PAGE_NOT_FOUND_TITLE_MARKER = 'Page Not Found'

  def extract_domain(self) -> str:
    parsed = urlparse(self.url)
    return parsed.netloc

  @classmethod
  def from_response(cls, resp: Response) -> Optional['NewsArticle']:
    article = Article(resp.url)
    article.set_html(resp.content)
    article.parse()

    if article.title == cls.PAGE_NOT_FOUND_TITLE_MARKER:
      return None

    if not article.authors:
      return None

    if not article.publish_date:
      return None

    if not article.text:
      return None

    return cls(url=resp.url,
               title=str(article.title),
               authors=list(article.authors),
               publish_date=article.publish_date,
               text=article.text)


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

    # At most 12 concurrent downloads at once!
    article_fetch_futures = queue.Queue(maxsize=12)

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


if __name__ == '__main__':
  main()
