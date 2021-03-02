import logging
import http

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.packages.urllib3.util.retry import Retry


DEFAULT_TIMEOUT = 5 # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    """
    Requests Transport Adapter (https://requests.readthedocs.io/en/master/user/advanced/#transport-adapters) for timeouts
    """
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

def create_requests_session(timeout=5, retries=3, debug=False):
    """Create a Requests session with sane defaults

    Defines a resilient requests session by setting a timeout, a retry policy,
    detailed debugging and automatically raises exceptions on failling requests
    for easier usage with `try/catch` blocks

    Args:
        timeout (int, optional): Maximum number of seconds to wait for a response. Defaults to 5.
        retries (int, optional): Maximum number of retries on failed requests. Defaults to 3.
        debug (bool, optional): Activates debug mode. Defaults to False.

    Returns:
        Session: Requests Session

    Reference: https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
    """
    session = Session()

    retry_strategy = Retry(
        total=retries,  
        status_forcelist=[413, 429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    adapter = TimeoutHTTPAdapter(max_retries=retry_strategy, timeout=timeout)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
    session.hooks["response"] = [assert_status_hook]

    if debug:
        http.client.HTTPConnection.debuglevel = 1

        # https://stackoverflow.com/a/16630836/4017403
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    return session
