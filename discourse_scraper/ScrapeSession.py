import requests
import time

class ScrapeSession(requests.Session):
    def __init__(self, max_retries=7, backoff_factor=2):
        super().__init__()
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def send(self, request, **kwargs):
        retries = 0
        while retries < self.max_retries:
            response = super().send(request, **kwargs)

            if response.status_code == 429:
                retries += 1

                if 'Retry-After' in response.headers:
                    wait_time = int(response.headers['Retry-After'])
                    print(f"[rate-limit-satisfier] the server told to wait {wait_time} second(s)")
                else:
                    wait_time = self.backoff_factor ** retries
                    print(f"[rate-limit-satisfier] the server told to wait without specifying how many time, retrying in {wait_time} second(s)")
                time.sleep(wait_time)
            else:
                return response

        # If max retries reached and still getting 429, raise an exception
        raise Exception(f"Max retries reached. Still receiving 429 status code.")