import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import load_yaml
from .exceptions import EmptyResponseError, SourceRequestError

_DEFAULT_TIMEOUT = 30


def build_session() -> requests.Session:
    http_config = load_yaml("sources").get("http", {})
    retry = Retry(
        total=http_config.get("retry_total", 5),
        backoff_factor=http_config.get("backoff_factor", 1.0),
        status_forcelist=http_config.get("status_forcelist", [429, 500, 502, 503, 504]),
        allowed_methods=frozenset(http_config.get("allowed_methods", ["GET", "POST", "HEAD", "OPTIONS"])),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    json_body=None,
    timeout: int | None = None,
    logger=None,
) -> requests.Response:
    timeout = timeout or _DEFAULT_TIMEOUT
    try:
        response = session.request(method, url, params=params, json=json_body, timeout=timeout)
    except (requests.ConnectionError, requests.Timeout, requests.exceptions.RetryError) as exc:
        raise SourceRequestError(f"{method} {url} failed: {exc}") from exc

    warning_header = response.headers.get("Warning")
    if warning_header and logger is not None:
        logger.warning("Warning header from %s: %s", url, warning_header)

    if not response.ok:
        body_snippet = response.text[:300]
        raise SourceRequestError(f"{method} {url} returned status {response.status_code}: {body_snippet}")

    if not response.content:
        raise EmptyResponseError(f"{method} {url} returned an empty response body")

    return response